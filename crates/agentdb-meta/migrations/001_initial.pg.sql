-- Consolidated initial control-plane schema for PostgreSQL (single migration).
-- Derived by applying the historical 001-031 pg migrations to a fresh
-- database and dumping the result (pg_dump --schema-only): there are no
-- deployed clusters to migrate, so the incremental history is collapsed
-- into this one file. schema_migrations is created/tracked by the runner.


CREATE FUNCTION public.agentdb_notify_nodes() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'UPDATE'
       AND to_jsonb(NEW) - 'last_heartbeat_ms' = to_jsonb(OLD) - 'last_heartbeat_ms' THEN
        RETURN NULL;
    END IF;
    PERFORM pg_notify('agentdb_nodes_changed',
                      COALESCE(NEW.node_id, OLD.node_id));
    RETURN NULL;
END;
$$;

CREATE FUNCTION public.agentdb_notify_shards() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    PERFORM pg_notify('agentdb_shards_changed',
                      COALESCE(NEW.shard_id::text, OLD.shard_id::text));
    RETURN NULL;
END;
$$;

CREATE FUNCTION public.agentdb_notify_tablet_replicas() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    PERFORM pg_notify('agentdb_tablets_changed',
                      COALESCE(NEW.tablet_id::text, OLD.tablet_id::text));
    RETURN NULL;
END;
$$;

CREATE FUNCTION public.agentdb_notify_tablets() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'UPDATE'
       AND to_jsonb(NEW) - 'lease_expires_at_ms' = to_jsonb(OLD) - 'lease_expires_at_ms' THEN
        RETURN NULL;
    END IF;
    PERFORM pg_notify('agentdb_tablets_changed',
                      COALESCE(NEW.tablet_id::text, OLD.tablet_id::text));
    RETURN NULL;
END;
$$;

CREATE TABLE public.cells (
    cell_id text NOT NULL,
    region text,
    domain_class text DEFAULT 'standard'::text NOT NULL,
    deployment_profile text DEFAULT 'single_az_cluster'::text NOT NULL,
    capacity_class text DEFAULT 'stable'::text NOT NULL,
    metadata_isolation_level text DEFAULT 'shared_logical'::text NOT NULL,
    state text DEFAULT 'serving'::text NOT NULL,
    metadata_namespace text,
    object_store_namespace text,
    rollout_version bigint DEFAULT 0 NOT NULL,
    created_at_ms bigint NOT NULL,
    updated_at_ms bigint NOT NULL
);

CREATE TABLE public.cluster_root (
    id bigint NOT NULL,
    cluster_id text NOT NULL,
    bootstrap_generation bigint NOT NULL,
    created_at_ms bigint NOT NULL,
    updated_at_ms bigint NOT NULL
);

CREATE TABLE public.config (
    key text NOT NULL,
    value text NOT NULL,
    updated_at_ms bigint NOT NULL
);

CREATE TABLE public.node_operational (
    node_id text NOT NULL,
    reported_at_ms bigint NOT NULL,
    summary text NOT NULL
);

CREATE TABLE public.nodes (
    node_id text NOT NULL,
    address text NOT NULL,
    state text NOT NULL,
    last_heartbeat_ms bigint NOT NULL,
    joined_at_ms bigint NOT NULL,
    mongo_address text,
    availability_zone text,
    region text,
    cell_id text DEFAULT 'default'::text NOT NULL,
    capacity_class text DEFAULT 'stable'::text NOT NULL
);

CREATE TABLE public.placement_scopes (
    placement_scope_id text NOT NULL,
    kind text NOT NULL,
    shard_count bigint NOT NULL,
    hash_seed text,
    state text NOT NULL,
    epoch bigint DEFAULT 0 NOT NULL,
    created_at_ms bigint NOT NULL,
    cell_id text DEFAULT 'default'::text NOT NULL
);

CREATE TABLE public.scheduler_leader (
    id bigint NOT NULL,
    node_id text NOT NULL,
    epoch bigint DEFAULT 0 NOT NULL,
    expires_at_ms bigint NOT NULL,
    acquired_at_ms bigint NOT NULL,
    CONSTRAINT scheduler_leader_id_check CHECK ((id = 1))
);

CREATE TABLE public.shards (
    shard_id bigint NOT NULL,
    tablet_id bigint,
    updated_at_ms bigint NOT NULL
);

CREATE TABLE public.store_incarnations (
    node_id text NOT NULL,
    store_incarnation text NOT NULL,
    state text NOT NULL,
    admitted_at_ms bigint NOT NULL,
    retired_at_ms bigint
);

CREATE TABLE public.tablet_movements (
    movement_id text NOT NULL,
    movement_type text NOT NULL,
    source_tablet_id bigint NOT NULL,
    destination_tablet_id bigint NOT NULL,
    placement_scope_id text NOT NULL,
    start_shard bigint NOT NULL,
    end_shard bigint NOT NULL,
    required_applied_index bigint NOT NULL,
    status text NOT NULL,
    created_at_ms bigint NOT NULL,
    updated_at_ms bigint NOT NULL,
    completed_at_ms bigint,
    CONSTRAINT tablet_movements_movement_type_check CHECK ((movement_type = ANY (ARRAY['split'::text, 'merge'::text, 'manual_split'::text, 'manual_merge'::text]))),
    CONSTRAINT tablet_movements_required_applied_index_check CHECK ((required_applied_index >= 0)),
    CONSTRAINT tablet_movements_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'completed'::text, 'cancelled'::text])))
);

CREATE TABLE public.tablet_ranges (
    placement_scope_id text NOT NULL,
    start_shard bigint NOT NULL,
    end_shard bigint NOT NULL,
    tablet_id bigint NOT NULL,
    map_epoch bigint DEFAULT 0 NOT NULL,
    updated_at_ms bigint NOT NULL
);

CREATE TABLE public.tablet_replicas (
    tablet_id bigint NOT NULL,
    node_id text NOT NULL,
    role text NOT NULL,
    state text DEFAULT 'active'::text NOT NULL,
    updated_at_ms bigint NOT NULL
);

CREATE TABLE public.tablet_snapshots (
    snapshot_id text NOT NULL,
    tablet_id bigint NOT NULL,
    source_node_id text NOT NULL,
    source_raft_node_id bigint NOT NULL,
    target_node_id text NOT NULL,
    target_raft_node_id bigint NOT NULL,
    snapshot_index bigint NOT NULL,
    snapshot_term bigint NOT NULL,
    size_bytes bigint NOT NULL,
    sha256_hex text NOT NULL,
    storage_uri text,
    status text NOT NULL,
    created_at_ms bigint NOT NULL,
    updated_at_ms bigint NOT NULL,
    expires_at_ms bigint
);

CREATE TABLE public.tablets (
    tablet_id bigint NOT NULL,
    owner_node_id text,
    epoch bigint DEFAULT 0 NOT NULL,
    lease_expires_at_ms bigint DEFAULT 0 NOT NULL,
    created_at_ms bigint NOT NULL,
    handoff_target_node_id text,
    placement_scope_id text DEFAULT 'default'::text NOT NULL,
    state text DEFAULT 'serving'::text NOT NULL,
    raft_group_id text,
    encryption_policy_version bigint DEFAULT 0 NOT NULL
);

CREATE TABLE public.tenant_policies (
    tenant_id text NOT NULL,
    placement_class text NOT NULL,
    placement_scope_id text NOT NULL,
    encryption_class text NOT NULL,
    key_policy_id text,
    encryption_policy_version bigint DEFAULT 0 NOT NULL,
    policy_version bigint DEFAULT 0 NOT NULL,
    updated_at_ms bigint NOT NULL,
    cell_id text DEFAULT 'default'::text NOT NULL,
    tier text DEFAULT 'free'::text NOT NULL,
    priority bigint DEFAULT 0 NOT NULL
);

ALTER TABLE ONLY public.cells
    ADD CONSTRAINT cells_pkey PRIMARY KEY (cell_id);

ALTER TABLE ONLY public.cluster_root
    ADD CONSTRAINT cluster_root_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.config
    ADD CONSTRAINT config_pkey PRIMARY KEY (key);

ALTER TABLE ONLY public.node_operational
    ADD CONSTRAINT node_operational_pkey PRIMARY KEY (node_id);

ALTER TABLE ONLY public.nodes
    ADD CONSTRAINT nodes_pkey PRIMARY KEY (node_id);

ALTER TABLE ONLY public.placement_scopes
    ADD CONSTRAINT placement_scopes_pkey PRIMARY KEY (placement_scope_id);

ALTER TABLE ONLY public.scheduler_leader
    ADD CONSTRAINT scheduler_leader_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.shards
    ADD CONSTRAINT shards_pkey PRIMARY KEY (shard_id);

ALTER TABLE ONLY public.store_incarnations
    ADD CONSTRAINT store_incarnations_pkey PRIMARY KEY (node_id, store_incarnation);

ALTER TABLE ONLY public.tablet_movements
    ADD CONSTRAINT tablet_movements_pkey PRIMARY KEY (movement_id);

ALTER TABLE ONLY public.tablet_ranges
    ADD CONSTRAINT tablet_ranges_pkey PRIMARY KEY (placement_scope_id, start_shard, end_shard);

ALTER TABLE ONLY public.tablet_replicas
    ADD CONSTRAINT tablet_replicas_pkey1 PRIMARY KEY (tablet_id, node_id);

ALTER TABLE ONLY public.tablet_snapshots
    ADD CONSTRAINT tablet_snapshots_pkey PRIMARY KEY (snapshot_id);

ALTER TABLE ONLY public.tablets
    ADD CONSTRAINT tablets_pkey PRIMARY KEY (tablet_id);

ALTER TABLE ONLY public.tenant_policies
    ADD CONSTRAINT tenant_policies_pkey PRIMARY KEY (tenant_id);

CREATE INDEX idx_nodes_cell ON public.nodes USING btree (cell_id);

CREATE INDEX idx_nodes_last_heartbeat ON public.nodes USING btree (last_heartbeat_ms);

CREATE INDEX idx_placement_scopes_cell ON public.placement_scopes USING btree (cell_id);

CREATE INDEX idx_tablet_movements_destination ON public.tablet_movements USING btree (destination_tablet_id, status);

CREATE INDEX idx_tablet_movements_status ON public.tablet_movements USING btree (status, updated_at_ms);

CREATE INDEX idx_tablet_ranges_lookup ON public.tablet_ranges USING btree (placement_scope_id, start_shard, end_shard);

CREATE INDEX idx_tablet_ranges_tablet ON public.tablet_ranges USING btree (tablet_id);

CREATE INDEX idx_tablet_snapshots_expiry ON public.tablet_snapshots USING btree (status, expires_at_ms);

CREATE INDEX idx_tablet_snapshots_tablet_target ON public.tablet_snapshots USING btree (tablet_id, target_node_id, snapshot_index DESC);

CREATE INDEX idx_tenant_policies_cell_scope ON public.tenant_policies USING btree (cell_id, placement_scope_id);

CREATE INDEX idx_tenant_policies_scope ON public.tenant_policies USING btree (placement_scope_id);

CREATE TRIGGER trg_agentdb_nodes_notify AFTER INSERT OR DELETE OR UPDATE ON public.nodes FOR EACH ROW EXECUTE FUNCTION public.agentdb_notify_nodes();

CREATE TRIGGER trg_agentdb_shards_notify AFTER INSERT OR DELETE OR UPDATE ON public.shards FOR EACH ROW EXECUTE FUNCTION public.agentdb_notify_shards();

CREATE TRIGGER trg_agentdb_tablets_notify AFTER INSERT OR DELETE OR UPDATE ON public.tablets FOR EACH ROW EXECUTE FUNCTION public.agentdb_notify_tablets();


-- Seed rows the historical migrations inserted on first boot: the default
-- placement scope and the default cell. (The 027 tablet_replicas backfill was
-- a one-time data migration off a legacy staging table and has no fresh-DB
-- equivalent, so it is intentionally omitted.)
INSERT INTO placement_scopes
    (placement_scope_id, kind, shard_count, hash_seed, state, epoch, created_at_ms)
VALUES
    ('default', 'shared_pool', 16384, NULL, 'serving', 0, 0)
ON CONFLICT (placement_scope_id) DO NOTHING;

INSERT INTO cells
    (cell_id, region, domain_class, deployment_profile, capacity_class,
     metadata_isolation_level, state, metadata_namespace, object_store_namespace,
     rollout_version, created_at_ms, updated_at_ms)
VALUES
    ('default', NULL, 'standard', 'single_az_cluster', 'stable',
     'shared_logical', 'serving', 'default', 'default', 0, 0, 0)
ON CONFLICT (cell_id) DO NOTHING;

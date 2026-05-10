CREATE SCHEMA IF NOT EXISTS agentdb_bench;

CREATE TABLE IF NOT EXISTS agentdb_bench.tenants (
  run_id text NOT NULL,
  tenant_id text NOT NULL,
  external_key text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, tenant_id)
);

CREATE TABLE IF NOT EXISTS agentdb_bench.agents (
  run_id text NOT NULL,
  tenant_id text NOT NULL,
  agent_id text NOT NULL,
  alias text NOT NULL,
  config jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, tenant_id, agent_id)
);

CREATE TABLE IF NOT EXISTS agentdb_bench.sessions (
  run_id text NOT NULL,
  tenant_id text NOT NULL,
  agent_id text NOT NULL,
  session_id text NOT NULL,
  alias text NOT NULL,
  next_seq bigint NOT NULL DEFAULT 0,
  fork_parent_id text,
  fork_at_seq bigint,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, tenant_id, session_id)
);

CREATE TABLE IF NOT EXISTS agentdb_bench.events (
  run_id text NOT NULL,
  tenant_id text NOT NULL,
  agent_id text NOT NULL,
  session_id text NOT NULL,
  seq bigint NOT NULL,
  kind text NOT NULL,
  payload jsonb NOT NULL,
  artifact_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
  source_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, tenant_id, session_id, seq)
);

CREATE INDEX IF NOT EXISTS events_recent_idx
  ON agentdb_bench.events (run_id, tenant_id, session_id, seq DESC);

CREATE TABLE IF NOT EXISTS agentdb_bench.records (
  run_id text NOT NULL,
  tenant_id text NOT NULL,
  record_id text NOT NULL,
  session_id text,
  namespace text[] NOT NULL,
  tags text[] NOT NULL,
  text text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, tenant_id, record_id)
);

CREATE INDEX IF NOT EXISTS records_scope_idx
  ON agentdb_bench.records (run_id, tenant_id, session_id);

CREATE INDEX IF NOT EXISTS records_tags_idx
  ON agentdb_bench.records USING gin (tags);

CREATE TABLE IF NOT EXISTS agentdb_bench.artifacts (
  run_id text NOT NULL,
  tenant_id text NOT NULL,
  artifact_id text NOT NULL,
  session_id text,
  content_uri text,
  content_type text,
  bytes bigint NOT NULL,
  sha256 text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, tenant_id, artifact_id)
);

CREATE INDEX IF NOT EXISTS artifacts_session_idx
  ON agentdb_bench.artifacts (run_id, tenant_id, session_id);

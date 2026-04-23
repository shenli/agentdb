//! `agentdb dump-config` — snapshot the effective configuration
//! every env var + default would produce, as JSON. Each entry
//! carries a `source` field so operators can see at a glance which
//! values came from the environment vs. which are falling back to
//! defaults.
//!
//! Extend by adding to the `KNOWN_KEYS` table below.

use serde::Serialize;

use crate::cli::DumpConfigOpts;

/// A single effective-config entry.
#[derive(Debug, Serialize)]
pub struct ConfigEntry {
    /// Env var name. What you'd set to override.
    pub key: &'static str,
    /// Human-readable group — rendered as `[ lease ]` etc. in
    /// future `--format=text` output.
    pub group: &'static str,
    /// Current effective value after env resolution. `None` if the
    /// env var is unset and the key has no default at the binary
    /// level (e.g. S3 creds the SDK resolves internally).
    pub value: Option<String>,
    /// Where the value came from.
    pub source: Source,
    /// The default value the binary uses when neither env nor flag
    /// is set. Unused keys (S3 creds, etc.) carry `None`.
    pub default: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Source {
    Default,
    Env,
}

struct Key {
    name: &'static str,
    group: &'static str,
    default: Option<&'static str>,
}

const KNOWN_KEYS: &[Key] = &[
    // Storage paths.
    Key {
        name: "AGENTDB_DATA",
        group: "storage",
        default: Some("./data/rocksdb"),
    },
    Key {
        name: "AGENTDB_OBJECTS",
        group: "storage",
        default: Some("./data/objects"),
    },
    Key {
        name: "AGENTDB_META_PATH",
        group: "storage",
        default: Some("./data/meta.sqlite"),
    },
    Key {
        name: "AGENTDB_OBJECT_STORE",
        group: "storage",
        default: Some("local"),
    },
    Key {
        name: "AGENTDB_S3_BUCKET",
        group: "storage",
        default: None,
    },
    Key {
        name: "AGENTDB_S3_REGION",
        group: "storage",
        default: Some("us-east-1"),
    },
    Key {
        name: "AGENTDB_S3_ENDPOINT",
        group: "storage",
        default: None,
    },
    Key {
        name: "AGENTDB_KV_BACKEND",
        group: "storage",
        default: Some("rocksdb"),
    },
    Key {
        name: "AGENTDB_SLATEDB_PATH",
        group: "storage",
        default: Some("agentdb/slate"),
    },
    // Server listeners.
    Key {
        name: "AGENTDB_ADDR",
        group: "server",
        default: Some("127.0.0.1:7878"),
    },
    Key {
        name: "AGENTDB_MONGO_ADDR",
        group: "server",
        default: Some("127.0.0.1:7879"),
    },
    Key {
        name: "AGENTDB_BOXES",
        group: "server",
        default: Some("4"),
    },
    Key {
        name: "AGENTDB_NODE_ID",
        group: "server",
        default: None,
    },
    Key {
        name: "AGENTDB_NODE_ADDRESS",
        group: "server",
        default: None,
    },
    Key {
        name: "AGENTDB_ADMISSION_CAPACITY",
        group: "admission",
        default: Some("64"),
    },
    // Lifecycle.
    Key {
        name: "AGENTDB_TICK_SECS",
        group: "lifecycle",
        default: Some("2"),
    },
    Key {
        name: "AGENTDB_T_COLD_SECS",
        group: "lifecycle",
        default: Some("60"),
    },
    Key {
        name: "AGENTDB_T_ARCHIVE_SECS",
        group: "lifecycle",
        default: Some("600"),
    },
    Key {
        name: "AGENTDB_SEGMENT_GC_TICK_SECS",
        group: "lifecycle",
        default: Some("5"),
    },
    Key {
        name: "AGENTDB_SEGMENT_GC_GRACE_SECS",
        group: "lifecycle",
        default: Some("30"),
    },
    Key {
        name: "AGENTDB_TICK_MS",
        group: "lifecycle",
        default: Some("1000"),
    },
    // Lease / heartbeat / election / reaper (M21-M22).
    Key {
        name: "AGENTDB_LEASE_DURATION_SECS",
        group: "lease",
        default: Some("30"),
    },
    Key {
        name: "AGENTDB_LEASE_RENEW_SECS",
        group: "lease",
        default: Some("10"),
    },
    Key {
        name: "AGENTDB_LEASE_SAFETY_MARGIN_SECS",
        group: "lease",
        default: Some("5"),
    },
    Key {
        name: "AGENTDB_HEARTBEAT_SECS",
        group: "heartbeat",
        default: Some("5"),
    },
    Key {
        name: "AGENTDB_LEADER_LEASE_SECS",
        group: "scheduler",
        default: Some("30"),
    },
    Key {
        name: "AGENTDB_LEADER_RENEW_SECS",
        group: "scheduler",
        default: Some("5"),
    },
    Key {
        name: "AGENTDB_LEADER_ATTEMPT_SECS",
        group: "scheduler",
        default: Some("2"),
    },
    Key {
        name: "AGENTDB_REAPER_TICK_SECS",
        group: "scheduler",
        default: Some("10"),
    },
    Key {
        name: "AGENTDB_REAPER_SAFETY_MARGIN_SECS",
        group: "scheduler",
        default: Some("5"),
    },
    // Observability.
    Key {
        name: "AGENTDB_OTLP_ENDPOINT",
        group: "observability",
        default: None,
    },
    Key {
        name: "AGENTDB_TRACE_SERVICE_NAME",
        group: "observability",
        default: Some("agentdb"),
    },
    Key {
        name: "RUST_LOG",
        group: "observability",
        default: Some("info"),
    },
];

pub fn run(opts: DumpConfigOpts) -> anyhow::Result<()> {
    let entries: Vec<ConfigEntry> = KNOWN_KEYS
        .iter()
        .map(|k| {
            let env_value = std::env::var(k.name).ok();
            let (value, source) = match (&env_value, k.default) {
                (Some(v), _) => (Some(v.clone()), Source::Env),
                (None, Some(d)) => (Some(d.to_string()), Source::Default),
                (None, None) => (None, Source::Default),
            };
            ConfigEntry {
                key: k.name,
                group: k.group,
                value,
                source,
                default: k.default.map(|s| s.to_string()),
            }
        })
        .collect();

    let mut preamble = serde_json::Map::new();
    preamble.insert(
        "dev_mode_preview".into(),
        serde_json::Value::Bool(opts.dev),
    );
    preamble.insert(
        "note".into(),
        serde_json::Value::String(
            "Source 'env' means the environment variable is set; 'default' means \
             AgentDB is using its built-in default. Flags on `agentdb serve` \
             override env values at runtime but are not reflected here because \
             `dump-config` does not accept the same flag surface."
                .into(),
        ),
    );
    let output = serde_json::json!({
        "meta": preamble,
        "entries": entries,
    });
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

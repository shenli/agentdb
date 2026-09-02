//! Import coding-agent session logs (Claude Code, Codex) into AgentDB.
//!
//! One JSONL file is one session; the repository path (the log's `cwd`) is the
//! workspace; the session UUID is the session alias; each line becomes an event
//! whose kind is the line's `type`. Appends carry an `Idempotency-Key` of
//! `<file>:<line>`, so re-running — and `--watch` — never duplicates an event.
//! A small local state file remembers each file's created ids and how many lines
//! have been imported, so re-runs only send new lines.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};

use crate::{Api, ImportArgs};

#[derive(Clone, Copy, Debug)]
pub(crate) enum Source {
    ClaudeCode,
    Codex,
}

impl Source {
    fn agent_alias(self) -> &'static str {
        match self {
            Source::ClaudeCode => "claude-code",
            Source::Codex => "codex",
        }
    }

    fn default_dir(self) -> Option<PathBuf> {
        let home = home()?;
        Some(match self {
            Source::ClaudeCode => home.join(".claude/projects"),
            Source::Codex => home.join(".codex/sessions"),
        })
    }
}

fn home() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

#[derive(Default)]
struct Summary {
    files_seen: usize,
    sessions_touched: usize,
    events_appended: usize,
    errors: usize,
}

pub(crate) async fn run(api: &Api, source: Source, args: ImportArgs) -> Result<Value> {
    let dir = args
        .dir
        .clone()
        .or_else(|| source.default_dir())
        .ok_or_else(|| anyhow!("could not resolve a source directory; pass --dir"))?;
    if !dir.exists() {
        anyhow::bail!("source directory {} does not exist", dir.display());
    }

    let mut state = State::load()?;
    let summary = import_once(api, source, &dir, &mut state).await?;

    if !args.watch {
        return Ok(summary.to_value(&dir));
    }

    eprintln!(
        "imported {} event(s); watching {} every {}ms (Ctrl-C to stop)",
        summary.events_appended,
        dir.display(),
        args.poll_ms
    );
    loop {
        tokio::time::sleep(Duration::from_millis(args.poll_ms)).await;
        match import_once(api, source, &dir, &mut state).await {
            Ok(s) if s.events_appended > 0 => {
                eprintln!(
                    "imported {} new event(s) across {} session(s)",
                    s.events_appended, s.sessions_touched
                );
            }
            Ok(_) => {}
            Err(error) => eprintln!("import pass failed (will retry): {error:#}"),
        }
    }
}

async fn import_once(
    api: &Api,
    source: Source,
    dir: &Path,
    state: &mut State,
) -> Result<Summary> {
    let mut summary = Summary::default();
    let mut files = Vec::new();
    collect_jsonl(dir, &mut files)?;
    files.sort();
    for file in files {
        summary.files_seen += 1;
        match import_file(api, source, &file, state).await {
            Ok(appended) => {
                if appended > 0 {
                    summary.sessions_touched += 1;
                    summary.events_appended += appended;
                }
            }
            Err(error) => {
                summary.errors += 1;
                eprintln!("skip {}: {error:#}", file.display());
            }
        }
    }
    state.save()?;
    Ok(summary)
}

async fn import_file(
    api: &Api,
    source: Source,
    file: &Path,
    state: &mut State,
) -> Result<usize> {
    let key = file.to_string_lossy().into_owned();
    let text = std::fs::read_to_string(file).with_context(|| format!("read {}", file.display()))?;
    let lines: Vec<&str> = text.lines().collect();

    let start = state.next_line(&key);
    if start >= lines.len() {
        return Ok(0); // nothing new
    }

    let (session_alias, workspace) = derive_identity(source, file, &lines)?;

    // Resolve (and cache) the workspace/agent/session ids for this file.
    let entry = if let Some(existing) = state.entry(&key) {
        existing
    } else {
        let tenant_id = ensure_workspace(api, &workspace).await?;
        let agent_id = ensure_agent(api, &tenant_id, source.agent_alias()).await?;
        let session_id = ensure_session(api, &tenant_id, &agent_id, &session_alias).await?;
        state.set_entry(&key, &tenant_id, &agent_id, &session_id)
    };

    let mut appended = 0;
    for (idx, line) in lines.iter().enumerate().skip(start) {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue, // skip a malformed line rather than abort the file
        };
        let kind = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("log")
            .to_string();
        let body = json!({
            "agent_id": entry.agent_id,
            "kind": kind,
            "payload": value,
        });
        let path = format!(
            "/api/tenants/{}/sessions/{}/events",
            entry.tenant_id, entry.session_id
        );
        let idempotency_key = format!("{key}:{idx}");
        api.post_idempotent(&path, body, &idempotency_key)
            .await
            .with_context(|| format!("append line {idx} of {}", file.display()))?;
        appended += 1;
        state.set_next_line(&key, idx + 1);
    }
    Ok(appended)
}

/// (session_alias, workspace) for a source's file.
fn derive_identity(source: Source, file: &Path, lines: &[&str]) -> Result<(String, String)> {
    let file_stem = file
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    let parent_name = file
        .parent()
        .and_then(|p| p.file_name())
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();

    match source {
        Source::ClaudeCode => {
            // Session id is the file name; workspace is the first line's cwd.
            let cwd = first_field(lines, |v| v.get("cwd").and_then(Value::as_str));
            let workspace = cwd.unwrap_or_else(|| decode_claude_project_dir(&parent_name));
            Ok((file_stem, workspace))
        }
        Source::Codex => {
            // The session_meta line carries the session id and cwd.
            let meta = lines.iter().find_map(|l| {
                let v: Value = serde_json::from_str(l).ok()?;
                (v.get("type").and_then(Value::as_str) == Some("session_meta"))
                    .then(|| v.get("payload").cloned().unwrap_or(Value::Null))
            });
            let session = meta
                .as_ref()
                .and_then(|m| m.get("id").and_then(Value::as_str).map(str::to_string))
                .unwrap_or_else(|| file_stem.clone());
            let workspace = meta
                .as_ref()
                .and_then(|m| m.get("cwd").and_then(Value::as_str).map(str::to_string))
                .unwrap_or_else(|| "unknown".to_string());
            Ok((session, workspace))
        }
    }
}

fn first_field(lines: &[&str], pick: impl Fn(&Value) -> Option<&str>) -> Option<String> {
    lines.iter().find_map(|l| {
        let v: Value = serde_json::from_str(l).ok()?;
        pick(&v).map(str::to_string)
    })
}

/// Best-effort decode of a Claude Code project directory name
/// (`-Users-me-work-repo`) back to a path when no `cwd` was found in the log.
fn decode_claude_project_dir(name: &str) -> String {
    if name.starts_with('-') {
        name.replacen('-', "/", 1).replace('-', "/")
    } else {
        name.to_string()
    }
}

fn collect_jsonl(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(dir).with_context(|| format!("read dir {}", dir.display()))? {
        let path = entry?.path();
        if path.is_dir() {
            collect_jsonl(&path, out)?;
        } else if path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
            out.push(path);
        }
    }
    Ok(())
}

async fn ensure_workspace(api: &Api, external_key: &str) -> Result<String> {
    let list = api.get("/api/tenants").await?;
    if let Some(found) = list["tenants"].as_array().and_then(|a| {
        a.iter()
            .find(|t| t["external_key"] == json!(external_key))
    }) {
        return id_field(found, "tenant_id");
    }
    let created = api
        .post("/api/tenants", json!({ "external_key": external_key, "policy": {} }))
        .await?;
    id_field(&created, "tenant_id")
}

async fn ensure_agent(api: &Api, tenant_id: &str, alias: &str) -> Result<String> {
    let list = api.get(&format!("/api/tenants/{tenant_id}/agents")).await?;
    if let Some(found) = list["agents"]
        .as_array()
        .and_then(|a| a.iter().find(|x| x["alias"] == json!(alias)))
    {
        return id_field(found, "agent_id");
    }
    let created = api
        .post(
            &format!("/api/tenants/{tenant_id}/agents"),
            json!({ "alias": alias, "config": {} }),
        )
        .await?;
    id_field(&created, "agent_id")
}

async fn ensure_session(api: &Api, tenant_id: &str, agent_id: &str, alias: &str) -> Result<String> {
    let list = api.get(&format!("/api/tenants/{tenant_id}/sessions")).await?;
    if let Some(found) = list["sessions"]
        .as_array()
        .and_then(|a| a.iter().find(|x| x["alias"] == json!(alias)))
    {
        return id_field(found, "session_id");
    }
    let created = api
        .post(
            &format!("/api/tenants/{tenant_id}/agents/{agent_id}/sessions"),
            json!({ "alias": alias }),
        )
        .await?;
    id_field(&created, "session_id")
}

fn id_field(value: &Value, field: &str) -> Result<String> {
    value[field]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("response missing {field}: {value}"))
}

// ----- local import state (offsets + resolved ids per file) -----

struct State {
    path: PathBuf,
    root: Value,
}

struct Entry {
    tenant_id: String,
    agent_id: String,
    session_id: String,
}

impl State {
    fn state_path() -> PathBuf {
        home()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".agentdb/agentdbctl-import-state.json")
    }

    fn load() -> Result<Self> {
        let path = Self::state_path();
        let root = match std::fs::read_to_string(&path) {
            Ok(text) => serde_json::from_str(&text).unwrap_or_else(|_| json!({ "files": {} })),
            Err(_) => json!({ "files": {} }),
        };
        Ok(Self { path, root })
    }

    fn save(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&self.path, serde_json::to_vec_pretty(&self.root)?)?;
        Ok(())
    }

    fn next_line(&self, key: &str) -> usize {
        self.root["files"][key]["next_line"]
            .as_u64()
            .unwrap_or(0) as usize
    }

    fn set_next_line(&mut self, key: &str, line: usize) {
        self.root["files"][key]["next_line"] = json!(line);
    }

    fn entry(&self, key: &str) -> Option<Entry> {
        let f = &self.root["files"][key];
        Some(Entry {
            tenant_id: f["tenant_id"].as_str()?.to_string(),
            agent_id: f["agent_id"].as_str()?.to_string(),
            session_id: f["session_id"].as_str()?.to_string(),
        })
    }

    fn set_entry(&mut self, key: &str, tenant_id: &str, agent_id: &str, session_id: &str) -> Entry {
        let existing_next = self.next_line(key);
        self.root["files"][key] = json!({
            "tenant_id": tenant_id,
            "agent_id": agent_id,
            "session_id": session_id,
            "next_line": existing_next,
        });
        Entry {
            tenant_id: tenant_id.to_string(),
            agent_id: agent_id.to_string(),
            session_id: session_id.to_string(),
        }
    }
}

impl Summary {
    fn to_value(&self, dir: &Path) -> Value {
        json!({
            "directory": dir.display().to_string(),
            "files_seen": self.files_seen,
            "sessions_touched": self.sessions_touched,
            "events_appended": self.events_appended,
            "errors": self.errors,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_claude_project_dir_recovers_a_path() {
        assert_eq!(
            decode_claude_project_dir("-Users-lishen-work-agentdb"),
            "/Users/lishen/work/agentdb"
        );
        assert_eq!(decode_claude_project_dir("plain"), "plain");
    }

    #[test]
    fn derive_identity_claude_uses_stem_and_cwd() {
        let lines = [r#"{"type":"user","cwd":"/repo/x","sessionId":"abc"}"#];
        let (session, workspace) =
            derive_identity(Source::ClaudeCode, Path::new("/a/b/abc.jsonl"), &lines).unwrap();
        assert_eq!(session, "abc");
        assert_eq!(workspace, "/repo/x");
    }

    #[test]
    fn derive_identity_codex_uses_session_meta() {
        let lines =
            [r#"{"type":"session_meta","payload":{"id":"sid-1","cwd":"/repo/y"}}"#, r#"{"type":"event_msg","payload":{}}"#];
        let (session, workspace) =
            derive_identity(Source::Codex, Path::new("/a/rollout-x.jsonl"), &lines).unwrap();
        assert_eq!(session, "sid-1");
        assert_eq!(workspace, "/repo/y");
    }
}

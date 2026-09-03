//! Shared parsing for coding-agent session-log import (Claude Code, Codex).
//!
//! One JSONL file is one session; the repository path (the log's `cwd`) is the
//! workspace; the session UUID is the session alias; each line becomes an event
//! whose kind is the line's `type`. This crate is **sink-agnostic**: it turns
//! files into `(workspace, agent, session, events)` and tracks per-file import
//! offsets. Two sinks build on it — the HTTP client (`agentdbctl import`) and
//! the in-process runtime writer (`agentdb import`).

use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};

/// The runtime stores an event payload inline up to this many bytes; anything
/// larger belongs in an artifact, so importers skip it rather than fail.
pub const MAX_PAYLOAD_BYTES: usize = 65_536;

#[derive(Clone, Copy, Debug)]
pub enum Source {
    ClaudeCode,
    Codex,
}

impl Source {
    pub fn agent_alias(self) -> &'static str {
        match self {
            Source::ClaudeCode => "claude-code",
            Source::Codex => "codex",
        }
    }

    pub fn default_dir(self) -> Option<PathBuf> {
        let home = home()?;
        Some(match self {
            Source::ClaudeCode => home.join(".claude/projects"),
            Source::Codex => home.join(".codex/sessions"),
        })
    }
}

pub fn home() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

/// Recursively collect `*.jsonl` files under `dir`.
pub fn collect_jsonl(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
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

/// `(session_alias, workspace)` for a source's file.
pub fn derive_identity(source: Source, file: &Path, lines: &[&str]) -> Result<(String, String)> {
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

/// What one JSONL line becomes. Session logs can be tens of GB (embedded tool
/// output), so callers stream line by line and classify each — never load a
/// whole file — keeping memory bounded to one line plus one batch.
pub enum LineClass {
    /// An importable inline event.
    Event {
        kind: String,
        payload: Value,
        payload_bytes: usize,
    },
    /// Payload exceeds [`MAX_PAYLOAD_BYTES`]; belongs in an artifact, not an
    /// inline event, so it is skipped.
    Oversized,
    /// Blank or malformed line; skipped.
    Skip,
}

/// Classify a single JSONL line. Blank and malformed lines are `Skip`;
/// over-large payloads are `Oversized`; everything else is an `Event` whose
/// kind is the line's `type` and whose payload is the whole line.
pub fn classify_line(line: &str) -> LineClass {
    let line = line.trim();
    if line.is_empty() {
        return LineClass::Skip;
    }
    let value: Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(_) => return LineClass::Skip, // skip a malformed line rather than abort the file
    };
    let payload_bytes = serde_json::to_vec(&value).map(|v| v.len()).unwrap_or(usize::MAX);
    if payload_bytes > MAX_PAYLOAD_BYTES {
        return LineClass::Oversized;
    }
    let kind = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("log")
        .to_string();
    LineClass::Event {
        kind,
        payload: value,
        payload_bytes,
    }
}

/// Read up to `max_lines` lines from the head of a file, for identity
/// derivation (`session_meta` / first `cwd`) without loading the whole file.
pub fn read_prefix_lines(path: &Path, max_lines: usize) -> Result<Vec<String>> {
    use std::io::BufRead;
    let file =
        std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut out = Vec::new();
    for line in std::io::BufReader::new(file).lines() {
        if out.len() >= max_lines {
            break;
        }
        out.push(line?);
    }
    Ok(out)
}

// ----- local import state (offsets + resolved ids per file) -----

/// Per-file import checkpoint: how many lines of each file have been imported,
/// and the tenant/agent/session ids resolved for it. Lets re-runs (and
/// `--watch`) send only new lines. Persisted at `~/.agentdb/<name>.json`.
pub struct State {
    path: PathBuf,
    root: Value,
}

pub struct Entry {
    pub tenant_id: String,
    pub agent_id: String,
    pub session_id: String,
}

impl State {
    /// Load (or start) the checkpoint file named `file_name` under
    /// `~/.agentdb/` (e.g. `agentdbctl-import-state.json`).
    pub fn load(file_name: &str) -> Result<Self> {
        let path = home()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".agentdb")
            .join(file_name);
        let root = match std::fs::read_to_string(&path) {
            Ok(text) => serde_json::from_str(&text).unwrap_or_else(|_| json!({ "files": {} })),
            Err(_) => json!({ "files": {} }),
        };
        Ok(Self { path, root })
    }

    pub fn save(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&self.path, serde_json::to_vec_pretty(&self.root)?)?;
        Ok(())
    }

    pub fn next_line(&self, key: &str) -> usize {
        self.root["files"][key]["next_line"].as_u64().unwrap_or(0) as usize
    }

    pub fn set_next_line(&mut self, key: &str, line: usize) {
        self.root["files"][key]["next_line"] = json!(line);
    }

    pub fn entry(&self, key: &str) -> Option<Entry> {
        let f = &self.root["files"][key];
        Some(Entry {
            tenant_id: f["tenant_id"].as_str()?.to_string(),
            agent_id: f["agent_id"].as_str()?.to_string(),
            session_id: f["session_id"].as_str()?.to_string(),
        })
    }

    pub fn set_entry(
        &mut self,
        key: &str,
        tenant_id: &str,
        agent_id: &str,
        session_id: &str,
    ) -> Entry {
        self.root["files"][key]["tenant_id"] = json!(tenant_id);
        self.root["files"][key]["agent_id"] = json!(agent_id);
        self.root["files"][key]["session_id"] = json!(session_id);
        Entry {
            tenant_id: tenant_id.to_string(),
            agent_id: agent_id.to_string(),
            session_id: session_id.to_string(),
        }
    }
}

/// Resolve the source directory to import from.
pub fn resolve_dir(source: Source, explicit: Option<PathBuf>) -> Result<PathBuf> {
    let dir = explicit
        .or_else(|| source.default_dir())
        .ok_or_else(|| anyhow!("could not resolve a source directory; pass --dir"))?;
    if !dir.exists() {
        anyhow::bail!("source directory {} does not exist", dir.display());
    }
    Ok(dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn classify_handles_event_blank_malformed_and_oversized() {
        match classify_line(r#"{"type":"a","v":1}"#) {
            LineClass::Event { kind, payload_bytes, .. } => {
                assert_eq!(kind, "a");
                assert!(payload_bytes > 0);
            }
            _ => panic!("expected event"),
        }
        assert!(matches!(classify_line("   "), LineClass::Skip));
        assert!(matches!(classify_line("not json"), LineClass::Skip));
        let big = format!(r#"{{"type":"big","x":"{}"}}"#, "a".repeat(MAX_PAYLOAD_BYTES));
        assert!(matches!(classify_line(&big), LineClass::Oversized));
        // A line with no `type` still imports, kind defaults to "log".
        match classify_line(r#"{"v":1}"#) {
            LineClass::Event { kind, .. } => assert_eq!(kind, "log"),
            _ => panic!("expected event"),
        }
    }

    #[test]
    fn codex_identity_from_session_meta() {
        let lines = vec![
            r#"{"type":"session_meta","payload":{"id":"sess-1","cwd":"/work/repo"}}"#,
            r#"{"type":"event_msg","payload":{}}"#,
        ];
        let (session, workspace) =
            derive_identity(Source::Codex, Path::new("/a/rollout-x.jsonl"), &lines).unwrap();
        assert_eq!(session, "sess-1");
        assert_eq!(workspace, "/work/repo");
    }
}

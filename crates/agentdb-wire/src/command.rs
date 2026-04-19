use bson::{doc, Document};

use crate::error::{WireError, WireResult};

/// Extract the command name (first field of the body) and the target database (`$db`).
pub struct CommandHead<'a> {
    pub name: &'a str,
    pub db: Option<&'a str>,
}

pub fn inspect(body: &Document) -> WireResult<CommandHead<'_>> {
    let (name, _value) = body
        .iter()
        .next()
        .ok_or_else(|| WireError::Protocol("empty command body".into()))?;
    let db = body.get_str("$db").ok();
    Ok(CommandHead {
        name: name.as_str(),
        db,
    })
}

/// Turn any internal error into an `ok: 0` response document.
pub fn error_reply(err: &WireError) -> Document {
    doc! {
        "ok": 0.0,
        "errmsg": err.to_string(),
        "code": 1,
    }
}

pub fn not_implemented(cmd: &str) -> Document {
    doc! {
        "ok": 0.0,
        "errmsg": format!("{cmd} not implemented in M3"),
        "code": 59, // CommandNotFound
    }
}

use bson::Bson;

use crate::handlers::{browse, find as find_mod, hello as hello_mod, insert as insert_mod};
use crate::state::WireState;

pub async fn dispatch(body: &Document, state: &WireState, connection_id: i64) -> Document {
    match inspect(body) {
        Err(e) => error_reply(&e),
        Ok(head) => match head.name {
            "hello" | "isMaster" | "ismaster" => hello_mod::hello_response(connection_id),
            "ping" => browse::ping_response(),
            "buildInfo" | "buildinfo" => browse::build_info_response(),
            "listDatabases" => {
                match browse::list_databases_response(&state.registry, &state.tenants) {
                    Ok(d) => d,
                    Err(e) => error_reply(&e),
                }
            }
            "listCollections" => browse::list_collections_response(head.db.unwrap_or("")),
            "find" => {
                let collection = match body.get_str("find") {
                    Ok(s) => s.to_string(),
                    Err(_) => {
                        return error_reply(&crate::error::WireError::Protocol(
                            "find command missing collection name".into(),
                        ))
                    }
                };
                let filter = body.get_document("filter").cloned().unwrap_or_default();
                let limit = body.get_i64("limit").ok();
                match find_mod::find_response(
                    &state.registry,
                    &state.tenants,
                    &state.sessions,
                    head.db.unwrap_or(""),
                    &collection,
                    &filter,
                    limit,
                ) {
                    Ok(d) => d,
                    Err(e) => error_reply(&e),
                }
            }
            "insert" => {
                let collection = match body.get_str("insert") {
                    Ok(s) => s.to_string(),
                    Err(_) => {
                        return error_reply(&crate::error::WireError::Protocol(
                            "insert command missing collection name".into(),
                        ))
                    }
                };
                let docs_bson = body.get_array("documents").cloned().unwrap_or_default();
                let docs: Vec<Document> = docs_bson
                    .into_iter()
                    .filter_map(|b| match b {
                        Bson::Document(d) => Some(d),
                        _ => None,
                    })
                    .collect();
                match insert_mod::insert_response(
                    &state.registry,
                    &state.tenants,
                    &state.sessions,
                    head.db.unwrap_or(""),
                    &collection,
                    &docs,
                )
                .await
                {
                    Ok(d) => d,
                    Err(e) => error_reply(&e),
                }
            }
            other => not_implemented(other),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inspect_returns_command_name() {
        let body = doc! { "find": "events", "$db": "t-1", "filter": {} };
        let head = inspect(&body).unwrap();
        assert_eq!(head.name, "find");
        assert_eq!(head.db, Some("t-1"));
    }

    #[test]
    fn inspect_missing_db_is_ok() {
        let body = doc! { "hello": 1 };
        let head = inspect(&body).unwrap();
        assert_eq!(head.name, "hello");
        assert!(head.db.is_none());
    }

    #[test]
    fn error_reply_has_ok_zero_and_errmsg() {
        let err = WireError::Protocol("bad".into());
        let d = error_reply(&err);
        assert_eq!(d.get_f64("ok").unwrap(), 0.0);
        assert!(d.get_str("errmsg").unwrap().contains("bad"));
    }

    #[test]
    fn not_implemented_reply_carries_command_name() {
        let d = not_implemented("killCursors");
        assert!(d.get_str("errmsg").unwrap().contains("killCursors"));
    }

    use crate::state::WireState;
    use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
    use agentdb_storage::{KvStore, RocksKvStore};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn state() -> (TempDir, WireState) {
        let dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let registry = Arc::new(BoxRegistry::new(kv.clone(), 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv));
        (
            dir,
            WireState {
                registry,
                tenants,
                sessions,
            },
        )
    }

    #[tokio::test]
    async fn dispatch_hello_returns_ok_one() {
        let (_d, st) = state();
        let r = dispatch(&doc! { "hello": 1 }, &st, 7).await;
        assert_eq!(r.get_f64("ok").unwrap(), 1.0);
        assert_eq!(r.get_i64("connectionId").unwrap(), 7);
    }

    #[tokio::test]
    async fn dispatch_unknown_command_returns_not_implemented() {
        let (_d, st) = state();
        let r = dispatch(&doc! { "killCursors": 1 }, &st, 1).await;
        assert_eq!(r.get_f64("ok").unwrap(), 0.0);
        assert!(r.get_str("errmsg").unwrap().contains("killCursors"));
    }
}

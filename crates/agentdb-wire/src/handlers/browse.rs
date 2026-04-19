use agentdb_core::{BoxRegistry, TenantRegistry};
use bson::{doc, Bson, Document};

use crate::error::WireResult;

pub fn build_info_response() -> Document {
    doc! {
        "version": "7.0.0-agentdb",
        "versionArray": [7_i32, 0_i32, 0_i32, 0_i32],
        "gitVersion": "agentdb-prototype",
        "modules": Bson::Array(vec![]),
        "sysInfo": "deprecated",
        "allocator": "system",
        "javascriptEngine": "none",
        "bits": 64_i32,
        "debug": false,
        "maxBsonObjectSize": 16_777_216_i32,
        "storageEngines": Bson::Array(vec![Bson::String("rocksdb".into())]),
        "ok": 1.0,
    }
}

pub fn ping_response() -> Document {
    doc! { "ok": 1.0 }
}

/// Enumerate tenants as databases. Stubbed in T5; Task 6 restores the real body.
pub fn list_databases_response(
    _registry: &BoxRegistry,
    _tenants: &TenantRegistry,
) -> WireResult<Document> {
    // Real enumeration lands in Task 6.
    Ok(doc! {
        "databases": Bson::Array(vec![]),
        "totalSize": 0_i64,
        "ok": 1.0,
    })
}

/// Collections within a database are a fixed set in M3.
pub fn list_collections_response(db: &str) -> Document {
    let events = doc! { "name": "events", "type": "collection" };
    let sessions = doc! { "name": "sessions", "type": "collection" };
    let batch = vec![Bson::Document(events), Bson::Document(sessions)];
    doc! {
        "cursor": {
            "id": 0_i64,
            "ns": format!("{}.$cmd.listCollections", db),
            "firstBatch": Bson::Array(batch),
        },
        "ok": 1.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_info_has_version() {
        let r = build_info_response();
        assert!(r.get_str("version").unwrap().contains("agentdb"));
    }

    #[test]
    fn ping_ok() {
        assert_eq!(ping_response().get_f64("ok").unwrap(), 1.0);
    }

    #[test]
    fn list_collections_returns_events_and_sessions() {
        let r = list_collections_response("t-1");
        let cursor = r.get_document("cursor").unwrap();
        let batch = cursor.get_array("firstBatch").unwrap();
        let names: Vec<&str> = batch
            .iter()
            .map(|b| b.as_document().unwrap().get_str("name").unwrap())
            .collect();
        assert_eq!(names, vec!["events", "sessions"]);
    }
}

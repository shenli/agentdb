use agentdb_storage::keys::{
    agent_record_key, artifact_by_sha256_key, artifact_record_key, event_by_id_key, event_key,
    idempotency_key, record_by_namespace_key, record_key, session_record_key, tenant_record_key,
};

#[test]
fn tenant_key_format() {
    let k = tenant_record_key("ten_abc");
    assert_eq!(k, "record/tenant/ten_abc");
}

#[test]
fn agent_key_is_tenant_first() {
    let k = agent_record_key("ten_abc", "agt_xyz");
    assert_eq!(k, "record/agent/ten_abc/agt_xyz");
}

#[test]
fn session_key_is_tenant_first() {
    let k = session_record_key("ten_abc", "ses_xyz");
    assert_eq!(k, "record/session/ten_abc/ses_xyz");
}

#[test]
fn event_key_orders_by_seq() {
    let k1 = event_key("ten_a", "ses_s", 0);
    let k2 = event_key("ten_a", "ses_s", 1);
    assert!(k1 < k2);
}

#[test]
fn event_by_id_separate_lookup() {
    assert_eq!(event_by_id_key("ten_a", "evt_e"), "event_by_id/ten_a/evt_e");
}

#[test]
fn idempotency_key_includes_all_scope_components() {
    let k = idempotency_key("ten_a", "append", "ses_s", "client-key-1");
    assert_eq!(k, "idempotency/ten_a/append/ses_s/client-key-1");
}

#[test]
fn artifact_record_key_is_tenant_first() {
    let k = artifact_record_key("ten_abc", "art_xyz");
    assert_eq!(k, "record/artifact/ten_abc/art_xyz");
}

#[test]
fn artifact_by_sha256_key_is_tenant_scoped() {
    let k = artifact_by_sha256_key("ten_abc", "deadbeef");
    assert_eq!(k, "record/artifact_by_sha256/ten_abc/deadbeef");
}

#[test]
fn artifact_record_and_by_sha256_have_disjoint_prefixes() {
    // The hydration scan uses `record/artifact/` as a prefix; it must NOT
    // match the `record/artifact_by_sha256/...` index entries because the
    // byte after `record/artifact` is `_` not `/`.
    let rec = artifact_record_key("t", "a");
    let idx = artifact_by_sha256_key("t", "h");
    assert!(rec.starts_with("record/artifact/"));
    assert!(idx.starts_with("record/artifact_by_sha256/"));
    assert!(!idx.starts_with("record/artifact/"));
}

#[test]
fn record_key_is_tenant_first() {
    let k = record_key("ten_abc", "rec_xyz");
    assert_eq!(k, "record/searchable/ten_abc/rec_xyz");
}

#[test]
fn record_by_namespace_key_includes_namespace_path() {
    let k = record_by_namespace_key("ten_abc", "agent/support", "rec_xyz");
    assert_eq!(
        k,
        "record/searchable_by_namespace/ten_abc/agent/support/rec_xyz"
    );
}

#[test]
fn record_and_by_namespace_have_disjoint_prefixes() {
    // `scan_all_records` scans `record/searchable/` as a prefix; it must NOT
    // match the `record/searchable_by_namespace/...` secondary-index entries
    // because the byte after `record/searchable` is `_` for the index but
    // `/` for the record prefix.
    let rec = record_key("t", "r");
    let idx = record_by_namespace_key("t", "ns", "r");
    assert!(rec.starts_with("record/searchable/"));
    assert!(idx.starts_with("record/searchable_by_namespace/"));
    assert!(!idx.starts_with("record/searchable/"));
}

#[test]
fn record_key_disjoint_from_other_record_namespaces() {
    // The `record/searchable/...` prefix must not be a parent of (or be
    // shadowed by) any of the existing `record/{tenant,agent,session,artifact}/`
    // namespaces — we lean on this for clean prefix scans during hydration.
    let r = record_key("t", "r");
    assert!(!r.starts_with("record/tenant/"));
    assert!(!r.starts_with("record/agent/"));
    assert!(!r.starts_with("record/session/"));
    assert!(!r.starts_with("record/artifact/"));
}

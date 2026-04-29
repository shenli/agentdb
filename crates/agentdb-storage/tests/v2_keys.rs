use agentdb_storage::v2_keys::{
    agent_record_key, event_by_id_key, event_key, idempotency_key, session_record_key,
    tenant_record_key,
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

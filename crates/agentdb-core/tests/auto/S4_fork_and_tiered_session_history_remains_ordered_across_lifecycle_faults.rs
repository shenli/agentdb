// AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
// Scenario: S4 fork_and_tiered_session_history_remains_ordered_across_lifecycle_faults
// Falsifies: C5, C6
//
// REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

use agentdb_core::{
    runtime::{AppendRequest, FetchRequest, ForkSessionRequest, Runtime},
    session::Lifecycle,
};

#[tokio::test]
async fn fork_and_tiered_session_history_remains_ordered_across_lifecycle_faults() {
    let rt = Runtime::in_memory();
    let (tenant, agent, parent) = rt.bootstrap_test_session().await;

    for seq in 0..5_u64 {
        let appended = rt
            .append(AppendRequest::simple(
                &tenant,
                &agent,
                &parent,
                "parent",
                serde_json::json!({ "seq": seq }),
            ))
            .await
            .unwrap();
        assert_eq!(appended.seq, seq);
    }

    let child = rt
        .fork_session(ForkSessionRequest {
            tenant_id: tenant.tenant_id.clone(),
            session_id: parent.session_id.clone(),
            at_seq: 2,
            alias: Some("auto-child".into()),
        })
        .await
        .unwrap();
    for seq in 0..3_u64 {
        let appended = rt
            .append(AppendRequest::simple(
                &tenant,
                &agent,
                &child,
                "child",
                serde_json::json!({ "seq": seq }),
            ))
            .await
            .unwrap();
        assert_eq!(appended.seq, seq);
    }

    rt.transition_session_lifecycle(&tenant.tenant_id, &parent.session_id, Lifecycle::Cold)
        .await
        .unwrap();
    rt.transition_session_lifecycle(&tenant.tenant_id, &parent.session_id, Lifecycle::Archive)
        .await
        .unwrap();
    rt.restore_session_from_archive(&tenant.tenant_id, &parent.session_id, Lifecycle::Cold)
        .await
        .unwrap();
    let resumed = rt
        .append(AppendRequest::simple(
            &tenant,
            &agent,
            &parent,
            "parent-resumed",
            serde_json::json!({ "seq": 5 }),
        ))
        .await
        .unwrap();
    assert_eq!(resumed.seq, 5);

    let parent_events = rt
        .fetch_from_seq(FetchRequest {
            tenant_id: tenant.tenant_id.clone(),
            session_id: parent.session_id.clone(),
            from_seq: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(
        parent_events
            .iter()
            .map(|event| (event.kind.as_str(), event.seq))
            .collect::<Vec<_>>(),
        vec![
            ("parent", 0),
            ("parent", 1),
            ("parent", 2),
            ("parent", 3),
            ("parent", 4),
            ("parent-resumed", 5),
        ]
    );

    let child_replay = rt
        .replay_session(&tenant.tenant_id, &child.session_id)
        .await
        .unwrap();
    assert_eq!(
        child_replay
            .events
            .iter()
            .map(|event| (event.kind.as_str(), event.seq))
            .collect::<Vec<_>>(),
        vec![
            ("parent", 0),
            ("parent", 1),
            ("parent", 2),
            ("child", 0),
            ("child", 1),
            ("child", 2),
        ]
    );
}

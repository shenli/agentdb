//! Self-hosted metadata (stages 2–3): bearer tokens, session locators, and
//! the tenant registry live as records under the reserved system tenant.
//! These tests pin the storage semantics and the public-API isolation of
//! that tenant.

mod common;

use agentdb_api::{sysmeta, AppState};
use agentdb_core::{Runtime, SYSTEM_TENANT_EXTERNAL_KEY, SYSTEM_TENANT_ID};
use common::spawn_app_with_state;
use serde_json::json;

#[tokio::test]
async fn token_lifecycle_round_trips_through_records() {
    let runtime = Runtime::in_memory();

    assert_eq!(sysmeta::count_all_tokens(&runtime).await.unwrap(), 0);
    let minted = sysmeta::mint_token(&runtime, "ops", sysmeta::AdminScope::Admin, None, 1_000)
        .await
        .unwrap();
    assert!(minted.plaintext.starts_with(sysmeta::TOKEN_PREFIX));

    let verified = sysmeta::verify_token(&runtime, &minted.plaintext)
        .await
        .unwrap()
        .expect("minted token verifies");
    assert_eq!(verified.token_id, minted.token_id);
    assert_eq!(verified.scope, sysmeta::AdminScope::Admin);
    assert!(sysmeta::verify_token(&runtime, "agentdb_not-a-real-token")
        .await
        .unwrap()
        .is_none());

    let active = sysmeta::list_active_tokens(&runtime).await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].description, "ops");

    assert!(sysmeta::revoke_token(&runtime, &minted.token_id, 2_000)
        .await
        .unwrap());
    assert!(!sysmeta::revoke_token(&runtime, &minted.token_id, 2_100)
        .await
        .unwrap());
    assert!(sysmeta::verify_token(&runtime, &minted.plaintext)
        .await
        .unwrap()
        .is_none());
    assert!(sysmeta::list_active_tokens(&runtime)
        .await
        .unwrap()
        .is_empty());
    // Revoked tokens still count toward the bootstrap gate.
    assert_eq!(sysmeta::count_all_tokens(&runtime).await.unwrap(), 1);
}

#[tokio::test]
async fn tenant_scope_validation_still_enforced_at_mint() {
    let runtime = Runtime::in_memory();
    assert!(
        sysmeta::mint_token(&runtime, "x", sysmeta::AdminScope::Tenant, None, 1_000)
            .await
            .is_err()
    );
    assert!(sysmeta::mint_token(
        &runtime,
        "x",
        sysmeta::AdminScope::Admin,
        Some("ten_x"),
        1_000
    )
    .await
    .is_err());
}

#[tokio::test]
async fn locator_semantics_insert_if_absent_and_delete() {
    let runtime = Runtime::in_memory();

    assert!(
        sysmeta::upsert_locator(&runtime, "ses_1", "ten_1", Some(7), 3, 100)
            .await
            .unwrap()
    );
    // Insert-if-absent: a second upsert must NOT retarget the session.
    assert!(
        !sysmeta::upsert_locator(&runtime, "ses_1", "ten_1", Some(9), 3, 200)
            .await
            .unwrap()
    );
    let locator = sysmeta::get_locator(&runtime, "ses_1")
        .await
        .unwrap()
        .expect("locator");
    assert_eq!(locator.tablet_id, Some(7));
    assert_eq!(locator.tenant_external_id.as_deref(), Some("ten_1"));

    sysmeta::upsert_locator(&runtime, "ses_2", "ten_1", Some(7), 4, 100)
        .await
        .unwrap();
    sysmeta::upsert_locator(&runtime, "ses_3", "ten_2", Some(8), 5, 100)
        .await
        .unwrap();
    assert_eq!(sysmeta::list_all_locators(&runtime).await.unwrap().len(), 3);

    assert_eq!(
        sysmeta::delete_locators_by_tenant_external(&runtime, "ten_1")
            .await
            .unwrap(),
        2
    );
    assert!(sysmeta::get_locator(&runtime, "ses_1")
        .await
        .unwrap()
        .is_none());
    assert!(sysmeta::delete_locator(&runtime, "ses_3").await.unwrap());
    assert!(sysmeta::list_all_locators(&runtime)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn system_tenant_is_invisible_and_unreachable_publicly() {
    let runtime = Runtime::in_memory();
    // Materialize it the way real servers do.
    runtime.ensure_system_tenant().await.unwrap();

    // Hidden from listings.
    assert!(runtime
        .list_tenants()
        .await
        .iter()
        .all(|t| t.tenant_id.as_str() != SYSTEM_TENANT_ID));

    // Public creation of the reserved identity is rejected.
    assert!(runtime
        .create_tenant(agentdb_core::tenant::NewTenant {
            external_key: SYSTEM_TENANT_EXTERNAL_KEY.into(),
            policy: json!({}),
        })
        .await
        .is_err());

    // Every HTTP request targeting it is refused before routing.
    let app = spawn_app_with_state(AppState::new(runtime)).await;
    for path in [
        format!("/api/tenants/{SYSTEM_TENANT_ID}"),
        format!("/api/tenants/{SYSTEM_TENANT_ID}/records/search?limit=1"),
    ] {
        assert_eq!(
            app.client
                .get(format!("{}{path}", app.base))
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::FORBIDDEN,
            "{path}"
        );
    }
}

#[tokio::test]
async fn tenant_registry_is_idempotent() {
    let runtime = Runtime::in_memory();
    let first = sysmeta::get_or_create_registered_tenant(&runtime, "ten_ext", 100)
        .await
        .unwrap();
    let second = sysmeta::get_or_create_registered_tenant(&runtime, "ten_ext", 999)
        .await
        .unwrap();
    assert_eq!(first.external_id, second.external_id);
    assert_eq!(second.created_at_ms, 100, "creation time is stable");
}

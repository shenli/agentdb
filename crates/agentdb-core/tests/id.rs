use agentdb_core::id::{Id, IdKind};

#[test]
fn tenant_id_has_ten_prefix_and_uuidv7_body() {
    let id = Id::new(IdKind::Tenant);
    let s = id.as_str();
    assert!(s.starts_with("ten_"), "got {s}");
    assert_eq!(s.len(), 4 + 36, "uuidv7 string is 36 chars; got {s}");
}

#[test]
fn ids_round_trip_through_str() {
    let id = Id::new(IdKind::Session);
    let parsed = Id::parse(id.as_str()).unwrap();
    assert_eq!(parsed.kind(), IdKind::Session);
    assert_eq!(parsed.as_str(), id.as_str());
}

#[test]
fn parse_rejects_wrong_prefix() {
    let err = Id::parse("xyz_018f4a7e-4b8a-7000-8000-000000000000").unwrap_err();
    assert!(format!("{err}").contains("unknown prefix"));
}

#[test]
fn parse_rejects_wrong_kind() {
    let id = Id::new(IdKind::Tenant);
    let err = Id::parse_kind(id.as_str(), IdKind::Session).unwrap_err();
    assert!(format!("{err}").contains("expected Session"));
}

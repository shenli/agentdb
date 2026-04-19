use agentdb_storage::{BoxId, Namespace, SessionId, TenantId};

const NS_META: u8 = Namespace::Meta as u8;

const TAG_TENANT_BY_EXT: u8 = 0x01;
const TAG_SESSION_BY_EXT: u8 = 0x02;
const TAG_TENANT_RECORD: u8 = 0x03;
const TAG_SESSION_RECORD: u8 = 0x04;
const TAG_NEXT_TENANT_ID: u8 = 0x05;
const TAG_NEXT_SESSION_ID: u8 = 0x06;

pub fn key_tenant_by_external(box_id: BoxId, external: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + external.len());
    out.push(NS_META);
    out.push(TAG_TENANT_BY_EXT);
    out.extend_from_slice(&box_id.to_be_bytes());
    out.extend_from_slice(external.as_bytes());
    out
}

pub fn key_session_by_external(box_id: BoxId, tenant_id: TenantId, external: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + external.len());
    out.push(NS_META);
    out.push(TAG_SESSION_BY_EXT);
    out.extend_from_slice(&box_id.to_be_bytes());
    out.extend_from_slice(&tenant_id.to_be_bytes());
    out.extend_from_slice(external.as_bytes());
    out
}

pub fn key_tenant_record(box_id: BoxId, tenant_id: TenantId) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    out.push(NS_META);
    out.push(TAG_TENANT_RECORD);
    out.extend_from_slice(&box_id.to_be_bytes());
    out.extend_from_slice(&tenant_id.to_be_bytes());
    out
}

pub fn key_session_record(box_id: BoxId, tenant_id: TenantId, session_id: SessionId) -> Vec<u8> {
    let mut out = Vec::with_capacity(20);
    out.push(NS_META);
    out.push(TAG_SESSION_RECORD);
    out.extend_from_slice(&box_id.to_be_bytes());
    out.extend_from_slice(&tenant_id.to_be_bytes());
    out.extend_from_slice(&session_id.to_be_bytes());
    out
}

pub fn key_next_tenant_id(box_id: BoxId) -> Vec<u8> {
    let mut out = Vec::with_capacity(4);
    out.push(NS_META);
    out.push(TAG_NEXT_TENANT_ID);
    out.extend_from_slice(&box_id.to_be_bytes());
    out
}

pub fn key_next_session_id(box_id: BoxId, tenant_id: TenantId) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    out.push(NS_META);
    out.push(TAG_NEXT_SESSION_ID);
    out.extend_from_slice(&box_id.to_be_bytes());
    out.extend_from_slice(&tenant_id.to_be_bytes());
    out
}

pub(crate) fn prefix_tenant_by_external_for_box(box_id: BoxId) -> Vec<u8> {
    let mut out = Vec::with_capacity(4);
    out.push(NS_META);
    out.push(TAG_TENANT_BY_EXT);
    out.extend_from_slice(&box_id.to_be_bytes());
    out
}

pub(crate) fn prefix_session_by_external_for_tenant(box_id: BoxId, tenant_id: TenantId) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    out.push(NS_META);
    out.push(TAG_SESSION_BY_EXT);
    out.extend_from_slice(&box_id.to_be_bytes());
    out.extend_from_slice(&tenant_id.to_be_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_meta_keys_start_with_meta_namespace_byte() {
        assert_eq!(key_tenant_by_external(1, "t").first(), Some(&NS_META));
        assert_eq!(key_session_by_external(1, 2, "s").first(), Some(&NS_META));
        assert_eq!(key_tenant_record(1, 2).first(), Some(&NS_META));
        assert_eq!(key_session_record(1, 2, 3).first(), Some(&NS_META));
        assert_eq!(key_next_tenant_id(1).first(), Some(&NS_META));
        assert_eq!(key_next_session_id(1, 2).first(), Some(&NS_META));
    }

    #[test]
    fn tenant_by_external_encodes_box_and_name() {
        let k = key_tenant_by_external(0x0123, "t-42");
        assert_eq!(k[0], NS_META);
        assert_eq!(k[1], TAG_TENANT_BY_EXT);
        assert_eq!(&k[2..4], &[0x01, 0x23]);
        assert_eq!(&k[4..], b"t-42");
    }

    #[test]
    fn session_by_external_encodes_box_tenant_name() {
        let k = key_session_by_external(7, 0x0A0B0C0D0E0F1011, "s-1");
        assert_eq!(&k[0..2], &[NS_META, TAG_SESSION_BY_EXT]);
        assert_eq!(&k[2..4], &7u16.to_be_bytes());
        assert_eq!(&k[4..12], &0x0A0B0C0D0E0F1011u64.to_be_bytes());
        assert_eq!(&k[12..], b"s-1");
    }

    #[test]
    fn different_kinds_never_collide() {
        let a = key_tenant_record(1, 2);
        let b = key_next_session_id(1, 2);
        assert_ne!(a, b);
        assert_ne!(a[1], b[1]);
    }

    #[test]
    fn different_external_names_produce_different_keys() {
        let a = key_tenant_by_external(1, "foo");
        let b = key_tenant_by_external(1, "bar");
        assert_ne!(a, b);
    }

    #[test]
    fn prefix_tenant_by_external_matches_concrete_tenant_keys() {
        let prefix = prefix_tenant_by_external_for_box(5);
        let concrete = key_tenant_by_external(5, "t-a");
        assert!(concrete.starts_with(&prefix));
        let other = key_tenant_by_external(6, "t-a");
        assert!(!other.starts_with(&prefix));
    }
}

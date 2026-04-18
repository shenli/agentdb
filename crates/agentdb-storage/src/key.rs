use crate::error::{Error, Result};

pub type BoxId = u16;
pub type TenantId = u64;
pub type SessionId = u64;
pub type BranchId = u32;
pub type Seq = u64;

/// Namespace tag — the first byte of every key.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Namespace {
    Events = 0x01,
    State = 0x02,
    Secondary = 0x03,
    Meta = 0x04,
}

impl Namespace {
    pub fn from_byte(b: u8) -> Result<Self> {
        match b {
            0x01 => Ok(Self::Events),
            0x02 => Ok(Self::State),
            0x03 => Ok(Self::Secondary),
            0x04 => Ok(Self::Meta),
            other => Err(Error::Other(format!(
                "unknown namespace tag 0x{:02x}",
                other
            ))),
        }
    }

    pub fn as_byte(self) -> u8 {
        self as u8
    }
}

/// Fixed length of every key: 1 + 2 + 8 + 8 + 4 + 8 = 31 bytes.
pub const KEY_LEN: usize = 1 + 2 + 8 + 8 + 4 + 8;

/// Fully-decoded view of a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedKey {
    pub ns: Namespace,
    pub box_id: BoxId,
    pub tenant_id: TenantId,
    pub session_id: SessionId,
    pub branch_id: BranchId,
    pub seq: Seq,
}

/// Encode a fully-qualified key.
pub fn encode_key(
    ns: Namespace,
    box_id: BoxId,
    tenant_id: TenantId,
    session_id: SessionId,
    branch_id: BranchId,
    seq: Seq,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(KEY_LEN);
    out.push(ns.as_byte());
    out.extend_from_slice(&box_id.to_be_bytes());
    out.extend_from_slice(&tenant_id.to_be_bytes());
    out.extend_from_slice(&session_id.to_be_bytes());
    out.extend_from_slice(&branch_id.to_be_bytes());
    out.extend_from_slice(&seq.to_be_bytes());
    debug_assert_eq!(out.len(), KEY_LEN);
    out
}

/// Decode a fully-qualified key.
pub fn decode_key(key: &[u8]) -> Result<DecodedKey> {
    if key.len() != KEY_LEN {
        return Err(Error::Other(format!(
            "key has wrong length: got {}, expected {}",
            key.len(),
            KEY_LEN
        )));
    }
    let ns = Namespace::from_byte(key[0])?;
    let box_id = u16::from_be_bytes(key[1..3].try_into().unwrap());
    let tenant_id = u64::from_be_bytes(key[3..11].try_into().unwrap());
    let session_id = u64::from_be_bytes(key[11..19].try_into().unwrap());
    let branch_id = u32::from_be_bytes(key[19..23].try_into().unwrap());
    let seq = u64::from_be_bytes(key[23..31].try_into().unwrap());
    Ok(DecodedKey {
        ns,
        box_id,
        tenant_id,
        session_id,
        branch_id,
        seq,
    })
}

pub fn prefix_for_box(ns: Namespace, box_id: BoxId) -> Vec<u8> {
    let mut out = Vec::with_capacity(3);
    out.push(ns.as_byte());
    out.extend_from_slice(&box_id.to_be_bytes());
    out
}

pub fn prefix_for_tenant(ns: Namespace, box_id: BoxId, tenant_id: TenantId) -> Vec<u8> {
    let mut out = Vec::with_capacity(11);
    out.push(ns.as_byte());
    out.extend_from_slice(&box_id.to_be_bytes());
    out.extend_from_slice(&tenant_id.to_be_bytes());
    out
}

pub fn prefix_for_session(
    ns: Namespace,
    box_id: BoxId,
    tenant_id: TenantId,
    session_id: SessionId,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(19);
    out.push(ns.as_byte());
    out.extend_from_slice(&box_id.to_be_bytes());
    out.extend_from_slice(&tenant_id.to_be_bytes());
    out.extend_from_slice(&session_id.to_be_bytes());
    out
}

pub fn prefix_for_session_branch(
    ns: Namespace,
    box_id: BoxId,
    tenant_id: TenantId,
    session_id: SessionId,
    branch_id: BranchId,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(23);
    out.push(ns.as_byte());
    out.extend_from_slice(&box_id.to_be_bytes());
    out.extend_from_slice(&tenant_id.to_be_bytes());
    out.extend_from_slice(&session_id.to_be_bytes());
    out.extend_from_slice(&branch_id.to_be_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_roundtrip_all_variants() {
        for ns in [
            Namespace::Events,
            Namespace::State,
            Namespace::Secondary,
            Namespace::Meta,
        ] {
            let byte = ns.as_byte();
            assert_eq!(Namespace::from_byte(byte).unwrap(), ns);
        }
    }

    #[test]
    fn from_byte_rejects_unknown_tag() {
        let err = Namespace::from_byte(0xFF).unwrap_err();
        assert!(err.to_string().contains("0xff"));
    }

    #[test]
    fn encode_decode_roundtrip() {
        let encoded = encode_key(
            Namespace::Events,
            42,        // box
            1_000_000, // tenant
            7,         // session
            3,         // branch
            99,        // seq
        );
        assert_eq!(encoded.len(), KEY_LEN);

        let decoded = decode_key(&encoded).unwrap();
        assert_eq!(decoded.ns, Namespace::Events);
        assert_eq!(decoded.box_id, 42);
        assert_eq!(decoded.tenant_id, 1_000_000);
        assert_eq!(decoded.session_id, 7);
        assert_eq!(decoded.branch_id, 3);
        assert_eq!(decoded.seq, 99);
    }

    #[test]
    fn decode_rejects_wrong_length() {
        let err = decode_key(&[0x01, 0x02, 0x03]).unwrap_err();
        assert!(err.to_string().contains("length"));
    }

    #[test]
    fn decode_rejects_unknown_namespace() {
        let mut key = vec![0u8; KEY_LEN];
        key[0] = 0x77;
        let err = decode_key(&key).unwrap_err();
        assert!(err.to_string().contains("namespace"));
    }

    #[test]
    fn prefix_for_box_is_prefix_of_full_key() {
        let full = encode_key(Namespace::Events, 42, 1, 2, 3, 4);
        let prefix = prefix_for_box(Namespace::Events, 42);
        assert_eq!(prefix.len(), 3);
        assert!(full.starts_with(&prefix));
    }

    #[test]
    fn prefix_for_tenant_is_prefix_of_full_key() {
        let full = encode_key(Namespace::State, 17, 99, 2, 3, 4);
        let prefix = prefix_for_tenant(Namespace::State, 17, 99);
        assert_eq!(prefix.len(), 11);
        assert!(full.starts_with(&prefix));
    }

    #[test]
    fn prefix_for_session_is_prefix_of_full_key() {
        let full = encode_key(Namespace::Meta, 1, 2, 333, 4, 5);
        let prefix = prefix_for_session(Namespace::Meta, 1, 2, 333);
        assert_eq!(prefix.len(), 19);
        assert!(full.starts_with(&prefix));
    }

    #[test]
    fn prefix_for_session_branch_is_prefix_of_full_key() {
        let full = encode_key(Namespace::Events, 1, 2, 3, 77, 5);
        let prefix = prefix_for_session_branch(Namespace::Events, 1, 2, 3, 77);
        assert_eq!(prefix.len(), 23);
        assert!(full.starts_with(&prefix));
    }

    #[test]
    fn prefix_for_box_does_not_match_different_box() {
        let other = encode_key(Namespace::Events, 43, 1, 2, 3, 4);
        let prefix = prefix_for_box(Namespace::Events, 42);
        assert!(!other.starts_with(&prefix));
    }
}

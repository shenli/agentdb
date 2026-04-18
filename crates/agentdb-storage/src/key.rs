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
}

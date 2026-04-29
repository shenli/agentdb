use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum IdKind {
    Tenant,
    Agent,
    Session,
    Event,
    Artifact,
    Record,
    Skill,
    Policy,
}

impl IdKind {
    pub fn prefix(&self) -> &'static str {
        match self {
            IdKind::Tenant => "ten",
            IdKind::Agent => "agt",
            IdKind::Session => "ses",
            IdKind::Event => "evt",
            IdKind::Artifact => "art",
            IdKind::Record => "rec",
            IdKind::Skill => "skl",
            IdKind::Policy => "pol",
        }
    }

    pub fn from_prefix(prefix: &str) -> Option<Self> {
        Some(match prefix {
            "ten" => IdKind::Tenant,
            "agt" => IdKind::Agent,
            "ses" => IdKind::Session,
            "evt" => IdKind::Event,
            "art" => IdKind::Artifact,
            "rec" => IdKind::Record,
            "skl" => IdKind::Skill,
            "pol" => IdKind::Policy,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Id {
    kind: IdKind,
    inner: String, // already includes the "kind_" prefix
}

#[derive(Debug, thiserror::Error)]
pub enum IdError {
    #[error("missing '_' separator in id")]
    MissingSeparator,
    #[error("unknown prefix '{0}'")]
    UnknownPrefix(String),
    #[error("invalid uuid body: {0}")]
    BadBody(#[from] uuid::Error),
    #[error("expected {expected:?} but got {got:?}")]
    KindMismatch { expected: IdKind, got: IdKind },
}

impl Id {
    pub fn new(kind: IdKind) -> Self {
        let body = Uuid::now_v7();
        let inner = format!("{}_{body}", kind.prefix());
        Self { kind, inner }
    }

    pub fn parse(s: &str) -> Result<Self, IdError> {
        let (prefix, body) = s.split_once('_').ok_or(IdError::MissingSeparator)?;
        let kind = IdKind::from_prefix(prefix)
            .ok_or_else(|| IdError::UnknownPrefix(prefix.to_string()))?;
        Uuid::from_str(body)?;
        Ok(Self { kind, inner: s.to_string() })
    }

    pub fn parse_kind(s: &str, expected: IdKind) -> Result<Self, IdError> {
        let id = Self::parse(s)?;
        if id.kind != expected {
            return Err(IdError::KindMismatch { expected, got: id.kind });
        }
        Ok(id)
    }

    pub fn kind(&self) -> IdKind { self.kind }
    pub fn as_str(&self) -> &str { &self.inner }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(&self.inner) }
}

impl serde::Serialize for Id {
    fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        ser.serialize_str(&self.inner)
    }
}

impl<'de> serde::Deserialize<'de> for Id {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;
        let s = String::deserialize(de)?;
        Id::parse(&s).map_err(D::Error::custom)
    }
}

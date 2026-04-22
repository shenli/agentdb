//! AgentDB in-memory indexing primitives.
//!
//! Provides `TextIndex` and `VectorIndex` traits and simple in-memory
//! implementations used by the box runtime to score events at query time.

pub mod doc_id;
pub mod error;
pub mod text;
pub mod vector;

pub use doc_id::DocId;
pub use error::{IndexError, IndexResult};
pub use text::{PathSnapshot, TextSnapshot};
pub use vector::{VectorPathSnapshot, VectorSnapshot};

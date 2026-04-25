//! Cross-node Mongo wire forwarding.
//!
//! When the local wire handler determines the target session
//! lives on a peer node, it relays the entire OP_MSG frame to
//! the peer and streams the response back. The peer treats the
//! forwarded command like any other client connection — so the
//! peer is responsible for routing, auth-checking, and replying.
//!
//! Connection pooling: v1 opens a fresh TCP connection per
//! forward. Pooling is a natural follow-up when forward
//! volume makes the dial cost visible in latency.
//!
//! Auth: when `AGENTDB_REQUIRE_AUTH=true`, the peer expects
//! SASL on every connection. SASL state is per-connection
//! and does not transfer across a forward — so the higher
//! layer (dispatch) returns the NotWritablePrimary error
//! in auth-required mode instead of calling us. See the
//! caller-side check in `command.rs`.

use std::time::Duration;

use agentdb_core::{BoxRegistry, RouteDecision};
use bson::{doc, Bson, Document};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::error::{WireError, WireResult};
use crate::opmsg;

/// Default connect + round-trip timeout. Keeps a misbehaving
/// peer from blocking the local request indefinitely.
pub const DEFAULT_FORWARD_TIMEOUT: Duration = Duration::from_secs(30);

/// Dial `peer_addr`, write the raw OP_MSG bytes in `frame`,
/// read back the response OP_MSG frame, return its bytes.
///
/// `frame` must be a complete OP_MSG (its first 4 bytes are
/// the little-endian length). The returned bytes are the peer's
/// response, ready to relay to the original client.
pub async fn forward_op_msg(peer_addr: &str, frame: &[u8]) -> WireResult<Vec<u8>> {
    if frame.len() < opmsg::HEADER_LEN {
        return Err(WireError::Protocol(format!(
            "forward: outbound frame too short ({} bytes)",
            frame.len()
        )));
    }
    timeout(DEFAULT_FORWARD_TIMEOUT, forward_inner(peer_addr, frame))
        .await
        .map_err(|_| WireError::Protocol(format!("forward to {peer_addr} timed out")))?
}

async fn forward_inner(peer_addr: &str, frame: &[u8]) -> WireResult<Vec<u8>> {
    let mut stream = TcpStream::connect(peer_addr).await.map_err(|e| {
        WireError::Protocol(format!("forward: connect to {peer_addr}: {e}"))
    })?;
    // Nagle off: the frame is already one write; we want it on
    // the wire immediately so small commands don't eat the 40ms
    // delay.
    let _ = stream.set_nodelay(true);
    stream.write_all(frame).await.map_err(|e| {
        WireError::Protocol(format!("forward: write to {peer_addr}: {e}"))
    })?;
    stream.flush().await.ok();

    // Read the response: 4-byte length prefix first, then the
    // rest of the frame.
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.map_err(|e| {
        WireError::Protocol(format!("forward: read length from {peer_addr}: {e}"))
    })?;
    let total =
        ReadBytesExt::read_i32::<LittleEndian>(&mut Cursor::new(&len_buf[..])).map_err(|e| {
            WireError::Protocol(format!(
                "forward: parse response length from {peer_addr}: {e}"
            ))
        })?;
    if total < opmsg::HEADER_LEN as i32 {
        return Err(WireError::Protocol(format!(
            "forward: response from {peer_addr} too short: {total} bytes"
        )));
    }
    let mut rest = vec![0u8; total as usize - 4];
    stream.read_exact(&mut rest).await.map_err(|e| {
        WireError::Protocol(format!("forward: read body from {peer_addr}: {e}"))
    })?;
    let mut whole = Vec::with_capacity(total as usize);
    whole.extend_from_slice(&len_buf);
    whole.extend_from_slice(&rest);
    Ok(whole)
}

/// Build a synthetic Mongo `find` command pinned to one
/// session, send it to `peer_mongo_addr`, decode the peer's
/// response and return `cursor.firstBatch`.
///
/// Used by the unpinned-fanout path in `handlers::find` when
/// the locator says some sessions on the tenant live on peer
/// nodes. Issuing one pinned find per remote session reuses
/// the existing wire forwarding machinery (frame round-trip
/// plus the auth/SASL exemption) without inventing a new
/// internal protocol; the peer treats the synthetic frame
/// like any other client connection.
///
/// `limit` of `None` means the peer applies no client-cap;
/// the caller is expected to enforce the merged cap after
/// stitching peers' results into the local batch.
pub async fn pinned_find(
    peer_mongo_addr: &str,
    db: &str,
    collection: &str,
    session_id: &str,
    limit: Option<i64>,
    raw: bool,
) -> WireResult<Vec<Document>> {
    let mut body = doc! {
        "find": collection,
        "$db": db,
        "filter": { "session_id": session_id },
    };
    if let Some(l) = limit {
        body.insert("limit", l);
    }
    if raw {
        body.insert("raw", true);
    }
    let mut frame = Vec::new();
    // request_id 0 / response_to 0: the peer copies request_id
    // into response_to anyway, and we throw away the reply
    // header after decoding the body section.
    opmsg::encode_body_response(&mut frame, 0, 0, &body)?;
    let reply = forward_op_msg(peer_mongo_addr, &frame).await?;
    let msg = opmsg::decode(&reply)?;
    let body = msg
        .body()
        .ok_or_else(|| WireError::Protocol("forwarded find: response had no body".into()))?;
    if let Ok(ok) = body.get_f64("ok") {
        if ok != 1.0 {
            let errmsg = body.get_str("errmsg").unwrap_or("(no errmsg)");
            return Err(WireError::Protocol(format!(
                "forwarded find peer rejected: {errmsg}"
            )));
        }
    }
    let cursor = body.get_document("cursor").map_err(|_| {
        WireError::Protocol("forwarded find: response missing cursor".into())
    })?;
    let batch = cursor.get_array("firstBatch").map_err(|_| {
        WireError::Protocol("forwarded find: response missing cursor.firstBatch".into())
    })?;
    Ok(batch
        .iter()
        .filter_map(|b| match b {
            Bson::Document(d) => Some(d.clone()),
            _ => None,
        })
        .collect())
}

/// Decide whether an incoming OP_MSG should be forwarded to a
/// peer node. Returns `Some(peer_mongo_addr)` when the command
/// targets a session owned elsewhere AND the peer advertised
/// a Mongo wire address AND auth is not required (SASL state
/// can't cross the wire forward).
///
/// `None` means "handle locally" — the regular dispatch path
/// applies. Single-session commands are the ones we can route
/// with confidence; multi-session or unscoped commands fall
/// through to local handling, which will error if they can't
/// be served.
pub fn routing_decision(
    body: &Document,
    doc_seqs: &[(&str, &[Document])],
    registry: &BoxRegistry,
    auth_required: bool,
) -> Option<String> {
    // SASL state is per-connection and doesn't transfer across
    // a forward. If auth is required, keep returning the
    // NotWritablePrimary error from the handlers so drivers
    // can reconnect to the owner.
    if auth_required {
        return None;
    }
    let (cmd_name, _) = body.iter().next()?;
    let db = body.get_str("$db").ok()?;
    let session_id = match cmd_name.as_str() {
        "find" => top_level_session_id(body.get_document("filter").ok()?)?,
        "update" => updates_session_id(body, doc_seqs)?,
        "delete" => deletes_session_id(body, doc_seqs)?,
        "insert" => insert_session_id(body, doc_seqs)?,
        "aggregate" => aggregate_session_id(body)?,
        _ => return None,
    };
    match registry.route_session_for_io(db, &session_id) {
        RouteDecision::Remote { target, .. } => target.mongo_address.clone(),
        RouteDecision::Local(_) => None,
    }
}

fn top_level_session_id(filter: &Document) -> Option<String> {
    filter.get_str("session_id").ok().map(|s| s.to_string())
}

/// Pull the session_id out of an `update` command. Returns
/// `Some` only when every entry in `updates[]` targets the
/// same session — mixed-session updates fall through to local
/// handling where they're rejected anyway.
fn updates_session_id(body: &Document, doc_seqs: &[(&str, &[Document])]) -> Option<String> {
    let updates = extract_spec_array(body, doc_seqs, "updates");
    same_session_across(&updates, |spec| {
        spec.get_document("q")
            .ok()
            .and_then(top_level_session_id)
    })
}

/// Same shape as `updates_session_id` but for `delete`.
fn deletes_session_id(body: &Document, doc_seqs: &[(&str, &[Document])]) -> Option<String> {
    let deletes = extract_spec_array(body, doc_seqs, "deletes");
    same_session_across(&deletes, |spec| {
        spec.get_document("q")
            .ok()
            .and_then(top_level_session_id)
    })
}

fn insert_session_id(body: &Document, doc_seqs: &[(&str, &[Document])]) -> Option<String> {
    let docs = extract_spec_array(body, doc_seqs, "documents");
    same_session_across(&docs, |d| {
        d.get_str("session_id").ok().map(|s| s.to_string())
    })
}

fn aggregate_session_id(body: &Document) -> Option<String> {
    // Look at the first pipeline stage's filter, if any.
    let pipeline = body.get_array("pipeline").ok()?;
    let first = pipeline.first().and_then(|b| b.as_document())?;
    if let Ok(filter) = first.get_document("filter") {
        if let Some(s) = top_level_session_id(filter) {
            return Some(s);
        }
    }
    for (key, _) in first.iter() {
        if key == "$vectorSearch" || key == "$search" {
            if let Ok(inner) = first.get_document(key) {
                if let Ok(filter) = inner.get_document("filter") {
                    if let Some(s) = top_level_session_id(filter) {
                        return Some(s);
                    }
                }
            }
        }
    }
    None
}

fn extract_spec_array(
    body: &Document,
    doc_seqs: &[(&str, &[Document])],
    identifier: &str,
) -> Vec<Document> {
    if let Some(seq) = doc_seqs.iter().find(|(id, _)| *id == identifier) {
        return seq.1.to_vec();
    }
    body.get_array(identifier)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|b| match b {
            Bson::Document(d) => Some(d),
            _ => None,
        })
        .collect()
}

fn same_session_across<T, F: Fn(&T) -> Option<String>>(
    items: &[T],
    pull: F,
) -> Option<String> {
    let mut iter = items.iter();
    let first = pull(iter.next()?)?;
    for rest in iter {
        let other = pull(rest)?;
        if other != first {
            return None;
        }
    }
    Some(first)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;
    use std::sync::Arc;

    /// Bare-minimum echo server that answers every incoming
    /// OP_MSG with `{ok: 1, echoed_by: addr}`. Used to exercise
    /// the forward client without spinning up the full wire
    /// dispatch.
    async fn spawn_echo_peer() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    loop {
                        let mut len_buf = [0u8; 4];
                        if socket.read_exact(&mut len_buf).await.is_err() {
                            return;
                        }
                        let total =
                            ReadBytesExt::read_i32::<LittleEndian>(&mut Cursor::new(&len_buf[..]))
                                .unwrap() as usize;
                        let mut rest = vec![0u8; total - 4];
                        if socket.read_exact(&mut rest).await.is_err() {
                            return;
                        }
                        let mut reply = Vec::new();
                        opmsg::encode_body_response(&mut reply, 0, 0, &doc! { "ok": 1.0 })
                            .unwrap();
                        let _ = socket.write_all(&reply).await;
                    }
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn forward_round_trips_a_framed_reply() {
        let peer = spawn_echo_peer().await;
        let mut req = Vec::new();
        opmsg::encode_body_response(&mut req, 1, 0, &doc! { "hello": 1 }).unwrap();
        let reply = forward_op_msg(&peer, &req).await.unwrap();
        let msg = opmsg::decode(&reply).unwrap();
        let body = msg.body().unwrap();
        assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    }

    /// Peer that captures the request body and answers with
    /// a canned `cursor.firstBatch` of two events. Used to
    /// exercise `pinned_find`'s frame construction + response
    /// decoding without spinning up the full dispatcher.
    async fn spawn_find_peer() -> (String, Arc<tokio::sync::Mutex<Option<Document>>>) {
        let captured = Arc::new(tokio::sync::Mutex::new(None));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let cap = captured.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let cap = cap.clone();
                tokio::spawn(async move {
                    loop {
                        let mut len_buf = [0u8; 4];
                        if socket.read_exact(&mut len_buf).await.is_err() {
                            return;
                        }
                        let total =
                            ReadBytesExt::read_i32::<LittleEndian>(&mut Cursor::new(&len_buf[..]))
                                .unwrap() as usize;
                        let mut rest = vec![0u8; total - 4];
                        if socket.read_exact(&mut rest).await.is_err() {
                            return;
                        }
                        let mut whole = Vec::with_capacity(total);
                        whole.extend_from_slice(&len_buf);
                        whole.extend_from_slice(&rest);
                        let req = opmsg::decode(&whole).unwrap();
                        if let Some(b) = req.body() {
                            *cap.lock().await = Some(b.clone());
                        }
                        let reply_body = doc! {
                            "cursor": {
                                "id": 0_i64,
                                "ns": "t-fwd.events",
                                "firstBatch": [
                                    doc! { "session_id": "s-x", "seq": 1_i64, "ts": 1_i64,
                                           "payload": { "kind": "from-peer-1" } },
                                    doc! { "session_id": "s-x", "seq": 2_i64, "ts": 2_i64,
                                           "payload": { "kind": "from-peer-2" } },
                                ],
                            },
                            "ok": 1.0,
                        };
                        let mut reply = Vec::new();
                        opmsg::encode_body_response(&mut reply, 0, 0, &reply_body).unwrap();
                        let _ = socket.write_all(&reply).await;
                    }
                });
            }
        });
        (addr, captured)
    }

    #[tokio::test]
    async fn pinned_find_round_trips_through_a_peer() {
        let (peer, captured) = spawn_find_peer().await;
        let docs = pinned_find(&peer, "t-fwd", "events", "s-x", Some(50), false)
            .await
            .unwrap();
        assert_eq!(docs.len(), 2);
        let kinds: Vec<String> = docs
            .iter()
            .map(|d| {
                d.get_document("payload")
                    .unwrap()
                    .get_str("kind")
                    .unwrap()
                    .to_string()
            })
            .collect();
        assert_eq!(kinds, vec!["from-peer-1", "from-peer-2"]);
        // Verify the synthetic frame the peer received matches
        // the contract: pinned filter, $db, find collection, limit.
        let req = captured.lock().await.clone().unwrap();
        assert_eq!(req.get_str("find").unwrap(), "events");
        assert_eq!(req.get_str("$db").unwrap(), "t-fwd");
        assert_eq!(req.get_i64("limit").unwrap(), 50);
        let f = req.get_document("filter").unwrap();
        assert_eq!(f.get_str("session_id").unwrap(), "s-x");
        assert!(req.get_bool("raw").is_err(), "raw default must be omitted");
    }

    /// Peer that returns `ok: 0` with an errmsg. `pinned_find`
    /// must surface this as a `WireError::Protocol` rather than
    /// silently dropping the events.
    async fn spawn_error_peer() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut len_buf = [0u8; 4];
                    if socket.read_exact(&mut len_buf).await.is_err() {
                        return;
                    }
                    let total =
                        ReadBytesExt::read_i32::<LittleEndian>(&mut Cursor::new(&len_buf[..]))
                            .unwrap() as usize;
                    let mut rest = vec![0u8; total - 4];
                    if socket.read_exact(&mut rest).await.is_err() {
                        return;
                    }
                    let body = doc! { "ok": 0.0, "errmsg": "synthetic peer error" };
                    let mut reply = Vec::new();
                    opmsg::encode_body_response(&mut reply, 0, 0, &body).unwrap();
                    let _ = socket.write_all(&reply).await;
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn pinned_find_surfaces_peer_errors() {
        let peer = spawn_error_peer().await;
        let err = pinned_find(&peer, "db", "events", "s", None, false)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("synthetic peer error"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn forward_to_dead_address_errors_out() {
        // Address we never bind. Connect() should fail fast.
        let err = forward_op_msg("127.0.0.1:1", &[0u8; 16])
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("connect") || msg.contains("timed out"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn forward_rejects_short_frames_before_dialing() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let err = rt
            .block_on(forward_op_msg("127.0.0.1:1", &[0u8; 4]))
            .unwrap_err();
        assert!(err.to_string().contains("too short"));
    }
}

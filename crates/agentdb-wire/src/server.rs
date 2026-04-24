use std::io::Cursor;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use byteorder::{LittleEndian, ReadBytesExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::{rustls::ServerConfig, TlsAcceptor};

use crate::command::dispatch;
use crate::error::{WireError, WireResult};
use crate::opmsg;
use crate::state::WireState;

/// Accept plaintext connections and spawn per-connection tasks
/// until the listener is dropped.
pub async fn serve(listener: TcpListener, state: WireState) -> WireResult<()> {
    let next_id = Arc::new(AtomicI64::new(1));
    loop {
        let (socket, peer) = listener.accept().await?;
        let st = state.clone();
        let id = next_id.fetch_add(1, Ordering::SeqCst);
        tokio::spawn(async move {
            if let Err(err) = run_connection(socket, st, id).await {
                tracing::debug!(%peer, ?err, "wire connection closed with error");
            }
        });
    }
}

/// TLS-wrapped variant of [`serve`]. `tls_config` is an
/// `Arc<rustls::ServerConfig>` — typically shared with the
/// REST listener's `RustlsConfig::get_inner()` so both ports
/// present the same cert.
///
/// Each handshake runs in its own spawned task, so a slow or
/// misbehaving client can't tie up the accept loop — same
/// shape as the plaintext path.
pub async fn serve_tls(
    listener: TcpListener,
    tls_config: Arc<ServerConfig>,
    state: WireState,
) -> WireResult<()> {
    let acceptor = TlsAcceptor::from(tls_config);
    let next_id = Arc::new(AtomicI64::new(1));
    loop {
        let (socket, peer) = listener.accept().await?;
        let acceptor = acceptor.clone();
        let st = state.clone();
        let id = next_id.fetch_add(1, Ordering::SeqCst);
        tokio::spawn(async move {
            let tls = match acceptor.accept(socket).await {
                Ok(s) => s,
                Err(err) => {
                    // Handshake failure is a client concern —
                    // log at debug so a stream of bad clients
                    // doesn't spam the logs at warn.
                    tracing::debug!(%peer, ?err, "wire TLS handshake failed");
                    return;
                }
            };
            if let Err(err) = run_connection(tls, st, id).await {
                tracing::debug!(%peer, ?err, "wire TLS connection closed with error");
            }
        });
    }
}

async fn run_connection<S>(
    mut socket: S,
    state: WireState,
    connection_id: i64,
) -> WireResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    loop {
        let mut len_buf = [0u8; 4];
        if let Err(err) = socket.read_exact(&mut len_buf).await {
            if err.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(()); // peer closed cleanly
            }
            return Err(err.into());
        }
        let msg_len_i32 = ReadBytesExt::read_i32::<LittleEndian>(&mut Cursor::new(&len_buf[..]))?;
        if msg_len_i32 < opmsg::HEADER_LEN as i32 {
            return Err(WireError::Protocol(format!("short frame: {msg_len_i32}")));
        }
        // Matches hello's advertised maxMessageSizeBytes; rejects adversarial huge frames.
        const MAX_MSG: i32 = 48_000_000;
        if msg_len_i32 > MAX_MSG {
            return Err(WireError::Protocol(format!(
                "frame too large: {msg_len_i32}"
            )));
        }
        let msg_len = msg_len_i32 as usize;
        let mut buf = vec![0u8; msg_len];
        buf[..4].copy_from_slice(&len_buf);
        socket.read_exact(&mut buf[4..]).await?;

        let msg = opmsg::decode(&buf)?;
        let body = msg
            .body()
            .ok_or_else(|| WireError::Protocol("request has no body section".into()))?;
        // Collect Type-1 document sequences (e.g. the "documents" sequence for insert).
        let doc_seqs: Vec<(&str, &[bson::Document])> = msg
            .sections
            .iter()
            .filter_map(|s| match s {
                opmsg::Section::DocumentSequence {
                    identifier,
                    documents,
                } => Some((identifier.as_str(), documents.as_slice())),
                _ => None,
            })
            .collect();
        let response_doc = dispatch(body, &doc_seqs, &state, connection_id).await;
        let mut reply = Vec::with_capacity(64 + response_doc.len() * 16);
        opmsg::encode_body_response(
            &mut reply,
            msg.header.request_id,
            msg.header.request_id,
            &response_doc,
        )?;
        socket.write_all(&reply).await?;
        socket.flush().await?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn spawn_server() -> (String, WireState) {
        use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
        use agentdb_storage::{KvStore, RocksKvStore};
        use object_store::local::LocalFileSystem;
        use object_store::ObjectStore;
        use std::sync::Arc;
        let dir = tempfile::TempDir::new().unwrap();
        let obj_dir = tempfile::TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let registry = Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv));
        let state = WireState::new(registry, tenants, sessions);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let st = state.clone();
        tokio::spawn(async move {
            let _ = serve(listener, st).await;
        });
        // Leak the tempdirs so they live past this fn — real integration tests
        // use a separate fixture.
        std::mem::forget(dir);
        std::mem::forget(obj_dir);
        (format!("127.0.0.1:{}", addr.port()), state)
    }

    #[tokio::test]
    async fn raw_hello_round_trip() {
        let (addr, _state) = spawn_server().await;
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut req_bytes = Vec::new();
        opmsg::encode_body_response(&mut req_bytes, 1, 0, &doc! { "hello": 1, "$db": "admin" })
            .unwrap();
        stream.write_all(&req_bytes).await.unwrap();

        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.unwrap();
        let total = ReadBytesExt::read_i32::<LittleEndian>(&mut Cursor::new(&len_buf[..])).unwrap()
            as usize;
        let mut rest = vec![0u8; total - 4];
        stream.read_exact(&mut rest).await.unwrap();
        let mut whole = Vec::with_capacity(total);
        whole.extend_from_slice(&len_buf);
        whole.extend_from_slice(&rest);
        let msg = opmsg::decode(&whole).unwrap();
        let body = msg.body().unwrap();
        assert_eq!(body.get_f64("ok").unwrap(), 1.0);
        assert!(body.get_bool("isWritablePrimary").unwrap());
    }
}

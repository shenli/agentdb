//! Mongo wire TLS: spin up `serve_tls` with an ephemeral
//! self-signed cert, connect with tokio-rustls, and verify
//! a hello round-trip works through the TLS handshake.
//!
//! The mongodb crate's TLS support validates certs against a
//! trust root; an ephemeral self-signed cert can't satisfy
//! that without `tls.allowInvalidCertificates` (unavailable
//! in the rust driver) or a custom CA file. So the test uses
//! the raw framing from `server::tests::raw_hello_round_trip`
//! but wrapped in tokio-rustls.

use std::io::Cursor;
use std::sync::Arc;

use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use agentdb_wire::{serve_tls, WireState};
use byteorder::{LittleEndian, ReadBytesExt};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::TlsConnector;

fn install_crypto_provider() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

/// Generate a self-signed cert + the matching server config
/// and a client config that trusts the cert. Returns
/// (server_config, client_config) so the test can wire up
/// both ends against the same trust anchor.
fn make_tls_pair() -> (Arc<ServerConfig>, Arc<ClientConfig>) {
    install_crypto_provider();
    let sans = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let generated = rcgen::generate_simple_self_signed(sans).unwrap();

    let cert_der = CertificateDer::from(generated.cert.der().to_vec());
    let key_der = PrivateKeyDer::try_from(generated.key_pair.serialize_der()).unwrap();

    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key_der)
        .unwrap();

    let mut roots = RootCertStore::empty();
    roots.add(cert_der).unwrap();
    let client_config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    (Arc::new(server_config), Arc::new(client_config))
}

async fn spawn_tls_server() -> (TempDir, TempDir, u16, Arc<ClientConfig>) {
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    let dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    let registry = Arc::new(BoxRegistry::new(kv.clone(), store, 2).unwrap());
    let tenants = Arc::new(TenantRegistry::new(kv.clone()));
    let sessions = Arc::new(SessionRegistry::new(kv));
    let state = WireState::new(registry, tenants, sessions);

    let (server_cfg, client_cfg) = make_tls_pair();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let _ = serve_tls(listener, server_cfg, state).await;
    });
    (dir, obj_dir, port, client_cfg)
}

#[tokio::test]
async fn raw_hello_round_trip_over_tls() {
    let (_dir, _obj_dir, port, client_cfg) = spawn_tls_server().await;

    let connector = TlsConnector::from(client_cfg);
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let dns = ServerName::try_from("localhost").unwrap();
    let mut stream = connector.connect(dns, tcp).await.unwrap();

    // Encode a hello request using the same framing as
    // `server::tests::raw_hello_round_trip`.
    let mut req = Vec::new();
    agentdb_wire::opmsg::encode_body_response(
        &mut req,
        1,
        0,
        &bson::doc! { "hello": 1, "$db": "admin" },
    )
    .unwrap();
    stream.write_all(&req).await.unwrap();
    stream.flush().await.unwrap();

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.unwrap();
    let total =
        ReadBytesExt::read_i32::<LittleEndian>(&mut Cursor::new(&len_buf[..])).unwrap() as usize;
    let mut rest = vec![0u8; total - 4];
    stream.read_exact(&mut rest).await.unwrap();
    let mut whole = Vec::with_capacity(total);
    whole.extend_from_slice(&len_buf);
    whole.extend_from_slice(&rest);

    let msg = agentdb_wire::opmsg::decode(&whole).unwrap();
    let body = msg.body().unwrap();
    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    assert!(body.get_bool("isWritablePrimary").unwrap());
}

#[tokio::test]
async fn plain_tcp_client_never_completes_a_wire_conversation() {
    let (_dir, _obj_dir, port, _client_cfg) = spawn_tls_server().await;

    // A plain-TCP client sending a Mongo hello to a TLS
    // listener: the server sees "not a ClientHello" and
    // tears down the socket (with or without a TLS alert
    // first). Under no circumstance does the client get a
    // legitimate Mongo wire reply back. Assert the lack of
    // the positive signal, not any particular failure mode
    // of rustls's alert machinery.
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let mut req = Vec::new();
    agentdb_wire::opmsg::encode_body_response(
        &mut req,
        1,
        0,
        &bson::doc! { "hello": 1, "$db": "admin" },
    )
    .unwrap();
    let _ = stream.write_all(&req).await;
    // Try to read a full reply. With a strict deadline so
    // the test stays fast regardless of alert behavior.
    let mut whole = Vec::new();
    let _ = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        stream.read_to_end(&mut whole),
    )
    .await;
    // Either nothing came back (EOF immediately) or what
    // came back is NOT a parseable wire message.
    if whole.len() >= 4 {
        let parsed = agentdb_wire::opmsg::decode(&whole);
        assert!(
            parsed.is_err(),
            "plain-TCP client parsed a real wire reply from a TLS listener; \
             that's a TLS bypass ({} bytes)",
            whole.len(),
        );
    }
}

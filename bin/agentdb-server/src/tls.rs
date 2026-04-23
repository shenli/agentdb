//! TLS termination helpers for the REST listener.
//!
//! Two entry points:
//!
//! - [`load_tls_config`] builds a `RustlsConfig` from PEM
//!   files on disk. Used when the operator supplies a real
//!   cert via `--tls-cert` / `--tls-key` or the env
//!   variants.
//! - [`generate_dev_tls`] synthesizes an ephemeral
//!   self-signed cert at startup so `agentdb serve
//!   --dev-tls` can smoke-test the TLS code path without
//!   out-of-band cert provisioning. The cert is valid for
//!   `localhost` + `127.0.0.1`.
//!
//! In both cases the caller hands the `RustlsConfig` to
//! `axum_server::bind_rustls(addr, config).serve(app)` in
//! place of the plaintext `axum::serve`.

use std::path::Path;
use std::sync::Once;

use anyhow::{Context, Result};
use axum_server::tls_rustls::RustlsConfig;

/// rustls 0.23 requires an explicit `CryptoProvider`
/// install before any handshake. We install `aws-lc-rs` —
/// matches the crypto feature selected on the `rcgen`
/// dep so certs generated here are usable by the handshake
/// code. Called from both TLS entry points so callers who
/// only use one of them still get the side effect.
fn install_crypto_provider() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        // Ignore the error — `install_default` fails when
        // called a second time (e.g. tests that run both
        // helpers). `Once` already guards against that,
        // but being defensive means we survive a future
        // caller from another crate installing first.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

/// Load a TLS config from PEM files. Accepts PKCS#8 or RSA
/// keys; tries PKCS#8 first, then RSA. Errors with a
/// human-readable message that names the file if the
/// decode fails.
pub async fn load_tls_config(cert: &Path, key: &Path) -> Result<RustlsConfig> {
    install_crypto_provider();
    // axum-server's rustls helper handles the parsing + the
    // `rustls::ServerConfig` wiring for us.
    RustlsConfig::from_pem_file(cert, key)
        .await
        .with_context(|| {
            format!(
                "load TLS cert ({}) + key ({}) failed",
                cert.display(),
                key.display()
            )
        })
}

/// Build an ephemeral self-signed cert + key pair at
/// startup and return a `RustlsConfig` built from it. Not
/// persisted — each `agentdb serve --dev-tls` invocation
/// gets a fresh one. Valid SANs: `localhost`, `127.0.0.1`,
/// `::1`. Clients must skip certificate validation (the
/// cert isn't signed by any trust root).
pub async fn generate_dev_tls() -> Result<RustlsConfig> {
    install_crypto_provider();
    let sans = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "::1".to_string(),
    ];
    let cert = rcgen::generate_simple_self_signed(sans)
        .context("rcgen: generate_simple_self_signed")?;
    let cert_pem = cert.cert.pem().into_bytes();
    let key_pem = cert.key_pair.serialize_pem().into_bytes();
    RustlsConfig::from_pem(cert_pem, key_pem)
        .await
        .context("RustlsConfig from ephemeral PEM")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn generate_dev_tls_produces_a_valid_config() {
        let _cfg = generate_dev_tls().await.unwrap();
        // Nothing more to assert without an actual TLS
        // handshake; the fact that the rustls parse succeeded
        // is the signal.
    }

    #[tokio::test]
    async fn load_tls_config_errors_on_missing_file() {
        let err = load_tls_config(
            std::path::Path::new("/tmp/definitely-not-a-cert.pem"),
            std::path::Path::new("/tmp/definitely-not-a-key.pem"),
        )
        .await
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("definitely-not-a-cert"),
            "error should name the missing cert file: {msg}"
        );
    }

    /// Full-stack smoke test: bind an axum app on an
    /// ephemeral port using the dev cert, fire a real
    /// HTTPS request via reqwest (accepting the self-
    /// signed cert), and assert 200. Proves the cert is
    /// actually usable — a missing crypto provider or a
    /// bad key format shows up here, not just in parse.
    #[tokio::test]
    async fn dev_tls_handshakes_with_real_https_client() {
        use axum::routing::get;
        use axum::Router;

        let config = generate_dev_tls().await.unwrap();
        // Bind on an ephemeral port by probing the OS, then
        // hand the address (not the listener) to axum_server.
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe); // release before axum_server binds

        let app: Router = Router::new().route("/ping", get(|| async { "pong" }));
        let service = app.into_make_service();
        let server_handle = tokio::spawn(async move {
            axum_server::bind_rustls(addr, config)
                .serve(service)
                .await
        });

        // Brief wait so the listener is ready.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let client = reqwest::ClientBuilder::new()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();
        let resp = client
            .get(format!("https://{addr}/ping"))
            .send()
            .await
            .expect("reqwest should handshake against the dev cert");
        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(resp.text().await.unwrap(), "pong");

        // Proves server was TLS all along: a plain http
        // request to the same addr should NOT succeed.
        let plain = reqwest::get(format!("http://{addr}/ping")).await;
        assert!(
            plain.is_err(),
            "plain HTTP should not reach a TLS listener"
        );

        server_handle.abort();
    }
}

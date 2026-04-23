//! Server-side SCRAM-SHA-256 handshake over the MongoDB wire
//! protocol. Client issues `saslStart`, we reply with `server-first`
//! (salt + iteration count + combined nonce). Client replies with
//! `saslContinue` carrying the client proof; we verify and return
//! `server-final` (server signature) so the client can check the
//! server's identity.
//!
//! State diagram:
//!
//! ```text
//! (none)
//!   |  saslStart(mech=SCRAM-SHA-256, payload=client-first)
//!   v
//! AwaitingContinue {user, combined_nonce, server_first, conv_id}
//!   |  saslContinue(conversationId, payload=client-final)
//!   v
//! Authenticated {user_id, tenant_external}
//! ```
//!
//! Anything off the happy path (unknown user, malformed payload,
//! bad proof, etc.) removes the session from the map and returns
//! `code: 18 AuthenticationFailed` — no hint to the client about
//! which step failed (prevents user-enumeration probes).

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use bson::{doc, Binary, Bson, Document};
use rand::RngCore;

use agentdb_meta::{scram, users, users::User};

use crate::state::WireAuthConfig;

/// AuthenticationFailed — standard Mongo error code. All failure
/// paths return this; we never distinguish "bad user" from "bad
/// password" from "replay".
pub const CODE_AUTH_FAILED: i32 = 18;

pub enum SaslSession {
    /// Server has sent `server-first` and is awaiting the client's
    /// final message (containing the proof).
    AwaitingContinue {
        user: User,
        /// The exact bytes we sent as server-first. Stored so the
        /// auth-message construction in saslContinue uses the
        /// identical string.
        server_first: String,
        /// `client-first-bare` from saslStart — needed to build
        /// the auth message at saslContinue time.
        client_first_bare: String,
        /// Combined nonce (client + server). We verify the client
        /// echoed it verbatim in `r=` of client-final.
        combined_nonce: String,
        conv_id: i32,
    },
    /// Handshake finished successfully; subsequent commands on
    /// this connection can run.
    Authenticated {
        user_id: String,
        tenant_external: String,
    },
}

pub async fn handle_sasl_start(
    body: &Document,
    auth: &WireAuthConfig,
) -> (Document, Option<SaslSession>) {
    let mechanism = match body.get_str("mechanism") {
        Ok(s) => s,
        Err(_) => return (auth_failed_reply("missing mechanism"), None),
    };
    if mechanism != "SCRAM-SHA-256" {
        return (
            auth_failed_reply(&format!("unsupported mechanism: {mechanism}")),
            None,
        );
    }
    let payload = match read_payload_binary(body) {
        Ok(p) => p,
        Err(msg) => return (auth_failed_reply(msg), None),
    };
    let client_first = match std::str::from_utf8(&payload) {
        Ok(s) => s,
        Err(_) => return (auth_failed_reply("client-first not UTF-8"), None),
    };
    let parsed = match parse_client_first(client_first) {
        Ok(p) => p,
        Err(msg) => return (auth_failed_reply(msg), None),
    };

    // Look up the user. Treat "not found" identically to other
    // failures so an attacker can't enumerate valid usernames.
    let user = match users::get(auth.meta.pool(), parsed.username).await {
        Ok(Some(u)) => u,
        _ => return (auth_failed_reply("auth failed"), None),
    };

    // Build server-first: "r=<nonce>,s=<salt>,i=<iter>".
    let server_nonce_raw = random_nonce_raw();
    let server_nonce = B64.encode(&server_nonce_raw);
    let combined_nonce = format!("{}{}", parsed.client_nonce, server_nonce);
    let salt_b64 = B64.encode(&user.credentials.salt);
    let server_first = format!(
        "r={nonce},s={salt},i={iter}",
        nonce = combined_nonce,
        salt = salt_b64,
        iter = user.credentials.iterations,
    );

    let reply = doc! {
        "conversationId": 1_i32,
        "done": false,
        "payload": Bson::Binary(Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: server_first.as_bytes().to_vec(),
        }),
        "ok": 1.0,
    };
    let session = SaslSession::AwaitingContinue {
        user,
        server_first: server_first.clone(),
        client_first_bare: parsed.bare.to_string(),
        combined_nonce,
        conv_id: 1,
    };
    (reply, Some(session))
}

pub fn handle_sasl_continue(
    body: &Document,
    prior: SaslSession,
) -> (Document, Option<SaslSession>) {
    let (user, server_first, client_first_bare, combined_nonce, conv_id) = match prior {
        SaslSession::AwaitingContinue {
            user,
            server_first,
            client_first_bare,
            combined_nonce,
            conv_id,
        } => (user, server_first, client_first_bare, combined_nonce, conv_id),
        SaslSession::Authenticated { .. } => {
            // Client shouldn't re-send saslContinue after auth;
            // treat as a protocol error and drop the session.
            return (auth_failed_reply("unexpected saslContinue"), None);
        }
    };

    let req_conv = body.get_i32("conversationId").ok();
    if req_conv != Some(conv_id) {
        return (auth_failed_reply("conversationId mismatch"), None);
    }
    let payload = match read_payload_binary(body) {
        Ok(p) => p,
        Err(msg) => return (auth_failed_reply(msg), None),
    };
    let client_final = match std::str::from_utf8(&payload) {
        Ok(s) => s,
        Err(_) => return (auth_failed_reply("client-final not UTF-8"), None),
    };
    let (without_proof, proof_b64) = match split_client_final(client_final) {
        Some(p) => p,
        None => return (auth_failed_reply("malformed client-final"), None),
    };

    // Verify the nonce echo matches what we sent.
    let expected_r = format!("r={combined_nonce}");
    if !without_proof
        .split(',')
        .any(|p| p == expected_r.as_str())
    {
        return (auth_failed_reply("nonce mismatch"), None);
    }

    let proof = match B64.decode(proof_b64) {
        Ok(p) => p,
        Err(_) => return (auth_failed_reply("proof not base64"), None),
    };

    let auth_message = format!(
        "{client_first_bare},{server_first},{without_proof}",
        client_first_bare = client_first_bare,
        server_first = server_first,
        without_proof = without_proof,
    );
    let server_signature = match scram::verify_client_proof(
        &user.credentials,
        auth_message.as_bytes(),
        &proof,
    ) {
        Ok(sig) => sig,
        Err(_) => return (auth_failed_reply("auth failed"), None),
    };

    let server_final = format!("v={}", B64.encode(server_signature));
    let reply = doc! {
        "conversationId": conv_id,
        "done": true,
        "payload": Bson::Binary(Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: server_final.as_bytes().to_vec(),
        }),
        "ok": 1.0,
    };
    let session = SaslSession::Authenticated {
        user_id: user.user_id,
        tenant_external: user.tenant_external,
    };
    (reply, Some(session))
}

pub fn auth_failed_reply(msg: &str) -> Document {
    doc! {
        "ok": 0.0,
        "code": CODE_AUTH_FAILED,
        "errmsg": msg.to_string(),
    }
}

/// Read the `payload` field as raw bytes. Drivers always send it
/// as BSON binary; tests sometimes send it as a UTF-8 string, so
/// we accept both.
fn read_payload_binary(body: &Document) -> Result<Vec<u8>, &'static str> {
    match body.get("payload") {
        Some(Bson::Binary(b)) => Ok(b.bytes.clone()),
        Some(Bson::String(s)) => Ok(s.as_bytes().to_vec()),
        _ => Err("missing or malformed payload"),
    }
}

fn random_nonce_raw() -> [u8; 24] {
    let mut buf = [0u8; 24];
    rand::thread_rng().fill_bytes(&mut buf);
    buf
}

/// Parsed client-first message.
#[derive(Debug)]
struct ClientFirst<'a> {
    username: &'a str,
    client_nonce: &'a str,
    /// `client-first-bare` = the subset without the GS2 header,
    /// per RFC 5802. We store this for the auth-message later.
    bare: &'a str,
}

/// Parse `n,,n=user,r=<nonce>` (and variants). We support the
/// GS2 header `n,,` (no channel binding, no authzid) — that's
/// what MongoDB drivers send.
fn parse_client_first(s: &str) -> Result<ClientFirst<'_>, &'static str> {
    // GS2 header ends at the second comma.
    let (_gs2, rest) = split_at_once(s, ',').ok_or("missing gs2")?;
    let (_cbind, bare) = split_at_once(rest, ',').ok_or("missing cbind")?;

    let mut username = None;
    let mut client_nonce = None;
    for part in bare.split(',') {
        if let Some(u) = part.strip_prefix("n=") {
            username = Some(u);
        } else if let Some(r) = part.strip_prefix("r=") {
            client_nonce = Some(r);
        }
    }
    Ok(ClientFirst {
        username: username.ok_or("missing n=user")?,
        client_nonce: client_nonce.ok_or("missing r=nonce")?,
        bare,
    })
}

/// Split client-final on its `p=` section. Returns
/// `(client-final-without-proof, proof_b64)`.
fn split_client_final(s: &str) -> Option<(&str, &str)> {
    // Proof is the last `,p=...` section per RFC 5802.
    let idx = s.rfind(",p=")?;
    let without_proof = &s[..idx];
    let proof = &s[idx + 3..];
    Some((without_proof, proof))
}

fn split_at_once(s: &str, pat: char) -> Option<(&str, &str)> {
    s.find(pat).map(|i| (&s[..i], &s[i + 1..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_client_first_extracts_user_and_nonce() {
        let s = "n,,n=alice,r=rOprNGfwEbeRWgbNEkqO";
        let p = parse_client_first(s).unwrap();
        assert_eq!(p.username, "alice");
        assert_eq!(p.client_nonce, "rOprNGfwEbeRWgbNEkqO");
        assert_eq!(p.bare, "n=alice,r=rOprNGfwEbeRWgbNEkqO");
    }

    #[test]
    fn split_client_final_separates_proof() {
        let s = "c=biws,r=ABC123,p=proofbase64==";
        let (without, proof) = split_client_final(s).unwrap();
        assert_eq!(without, "c=biws,r=ABC123");
        assert_eq!(proof, "proofbase64==");
    }

    #[test]
    fn split_client_final_returns_none_when_missing() {
        assert!(split_client_final("c=biws,r=ABC").is_none());
    }
}

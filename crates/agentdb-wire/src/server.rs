use std::sync::Arc;

use agentdb_core::Runtime;
use bson::{doc, spec::BinarySubtype, Binary, Bson, Document};
use tokio::net::{TcpListener, TcpStream};

use crate::command::{command_name, CommandContext, CommandOptions};
use crate::error::WireResult;
use crate::opmsg::{read_op_msg, write_op_msg};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthConfig {
    pub username: String,
    pub password: String,
}

pub async fn serve(listener: TcpListener, runtime: Arc<Runtime>) -> WireResult<()> {
    serve_with_auth(listener, runtime, None).await
}

pub async fn serve_with_auth(
    listener: TcpListener,
    runtime: Arc<Runtime>,
    auth: Option<AuthConfig>,
) -> WireResult<()> {
    let ctx = CommandContext::new(runtime);
    loop {
        let (stream, peer) = listener.accept().await?;
        let ctx = ctx.clone();
        let auth = auth.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, ctx, auth).await {
                tracing::debug!(%peer, ?error, "MongoDB compatibility connection closed");
            }
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    ctx: CommandContext,
    auth: Option<AuthConfig>,
) -> WireResult<()> {
    let mut next_response_id = 1_i32;
    let mut authenticated = auth.is_none();
    while let Some(message) = read_op_msg(&mut stream).await? {
        let response = handle_message(&ctx, &auth, &mut authenticated, message.body)
            .await
            .unwrap_or_else(|error| {
                bson::doc! {
                    "ok": 0.0,
                    "errmsg": error.to_string(),
                    "code": 8_i32,
                    "codeName": "UnknownError",
                }
            });
        write_op_msg(&mut stream, next_response_id, message.request_id, response).await?;
        next_response_id = next_response_id.wrapping_add(1);
    }
    Ok(())
}

async fn handle_message(
    ctx: &CommandContext,
    auth: &Option<AuthConfig>,
    authenticated: &mut bool,
    request: Document,
) -> WireResult<Document> {
    let command = command_name(&request)?;
    if let Some(config) = auth {
        match command.as_str() {
            "hello" | "isMaster" | "ismaster" => {
                return ctx
                    .handle_with_options(request, CommandOptions { auth_enabled: true })
                    .await;
            }
            "saslStart" => return Ok(handle_sasl_start(config, authenticated, &request)),
            "saslContinue" => return Ok(handle_sasl_continue(*authenticated)),
            "endSessions" => return ctx.handle(request).await,
            _ if !*authenticated => return Ok(unauthorized_response()),
            _ => {}
        }
    }

    ctx.handle(request).await
}

fn handle_sasl_start(
    config: &AuthConfig,
    authenticated: &mut bool,
    request: &Document,
) -> Document {
    let mechanism = request.get_str("mechanism").unwrap_or("");
    if mechanism != "PLAIN" {
        return auth_failed_response("unsupported auth mechanism; use PLAIN");
    }
    let Some((username, password)) = plain_credentials(request) else {
        return auth_failed_response("invalid PLAIN authentication payload");
    };
    if username == config.username && password == config.password {
        *authenticated = true;
        doc! {
            "conversationId": 1_i32,
            "done": true,
            "payload": Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: Vec::new(),
            }),
            "ok": 1.0,
        }
    } else {
        auth_failed_response("authentication failed")
    }
}

fn handle_sasl_continue(authenticated: bool) -> Document {
    if authenticated {
        doc! {
            "conversationId": 1_i32,
            "done": true,
            "payload": Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: Vec::new(),
            }),
            "ok": 1.0,
        }
    } else {
        auth_failed_response("authentication failed")
    }
}

fn plain_credentials(request: &Document) -> Option<(String, String)> {
    let payload = request.get("payload")?;
    let bytes: Vec<u8> = match payload {
        Bson::Binary(binary) => binary.bytes.clone(),
        Bson::String(value) => value.as_bytes().to_vec(),
        _ => return None,
    };
    let parts = bytes.split(|byte| *byte == 0).collect::<Vec<_>>();
    if parts.len() < 3 {
        return None;
    }
    let username = String::from_utf8(parts[1].to_vec()).ok()?;
    let password = String::from_utf8(parts[2].to_vec()).ok()?;
    Some((username, password))
}

fn unauthorized_response() -> Document {
    doc! {
        "ok": 0.0,
        "errmsg": "MongoDB compatibility listener requires authentication",
        "code": 13_i32,
        "codeName": "Unauthorized",
    }
}

fn auth_failed_response(message: &str) -> Document {
    doc! {
        "ok": 0.0,
        "errmsg": message,
        "code": 18_i32,
        "codeName": "AuthenticationFailed",
    }
}

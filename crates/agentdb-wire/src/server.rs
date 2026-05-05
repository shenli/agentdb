use std::sync::Arc;

use agentdb_core::Runtime;
use tokio::net::{TcpListener, TcpStream};

use crate::command::CommandContext;
use crate::error::WireResult;
use crate::opmsg::{read_op_msg, write_op_msg};

pub async fn serve(listener: TcpListener, runtime: Arc<Runtime>) -> WireResult<()> {
    let ctx = CommandContext::new(runtime);
    loop {
        let (stream, peer) = listener.accept().await?;
        let ctx = ctx.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, ctx).await {
                tracing::debug!(%peer, ?error, "MongoDB compatibility connection closed");
            }
        });
    }
}

async fn handle_connection(mut stream: TcpStream, ctx: CommandContext) -> WireResult<()> {
    let mut next_response_id = 1_i32;
    while let Some(message) = read_op_msg(&mut stream).await? {
        let response = ctx.handle(message.body).await.unwrap_or_else(|error| {
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

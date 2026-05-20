use std::io::Cursor;

use bson::{Bson, Document};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{WireError, WireResult};

pub const OP_MSG: i32 = 2013;
pub const OP_QUERY: i32 = 2004;
pub const OP_REPLY: i32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseProtocol {
    OpMsg,
    OpReply,
}

#[derive(Debug, Clone)]
pub struct OpMsg {
    pub request_id: i32,
    pub response_to: i32,
    pub body: Document,
    pub response_protocol: ResponseProtocol,
}

pub async fn read_op_msg<R>(reader: &mut R) -> WireResult<Option<OpMsg>>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0u8; 16];
    match reader.read_exact(&mut header).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }

    let mut cur = Cursor::new(header);
    let message_len = ReadBytesExt::read_i32::<LittleEndian>(&mut cur)?;
    let request_id = ReadBytesExt::read_i32::<LittleEndian>(&mut cur)?;
    let response_to = ReadBytesExt::read_i32::<LittleEndian>(&mut cur)?;
    let op_code = ReadBytesExt::read_i32::<LittleEndian>(&mut cur)?;

    if message_len < 16 {
        return Err(WireError::Protocol(format!(
            "Mongo message too short: {message_len}"
        )));
    }

    let remaining_len = (message_len as usize).saturating_sub(16);
    let mut payload = vec![0u8; remaining_len];
    reader.read_exact(&mut payload).await?;

    match op_code {
        OP_MSG => read_op_msg_payload(request_id, response_to, payload),
        OP_QUERY => read_op_query_payload(request_id, response_to, payload),
        _ => Err(WireError::Protocol(format!(
            "unsupported Mongo opcode: {op_code}"
        ))),
    }
}

fn read_op_msg_payload(
    request_id: i32,
    response_to: i32,
    payload: Vec<u8>,
) -> WireResult<Option<OpMsg>> {
    if payload.len() < 5 {
        return Err(WireError::Protocol(format!(
            "Mongo OP_MSG payload too short: {}",
            payload.len()
        )));
    }
    let payload_len = payload.len() as u64;
    let mut cur = Cursor::new(payload);
    let _flag_bits = ReadBytesExt::read_i32::<LittleEndian>(&mut cur)?;
    let mut body = Document::new();
    while cur.position() < payload_len {
        let section_kind = ReadBytesExt::read_u8(&mut cur)?;
        match section_kind {
            0 => {
                let doc = Document::from_reader(&mut cur)?;
                body.extend(doc);
            }
            1 => {
                let section_start = cur.position();
                let section_size = ReadBytesExt::read_i32::<LittleEndian>(&mut cur)?;
                if section_size < 5 {
                    return Err(WireError::Protocol(format!(
                        "invalid OP_MSG document sequence size: {section_size}"
                    )));
                }
                let section_end = section_start + section_size as u64;
                let mut identifier = Vec::new();
                loop {
                    let byte = ReadBytesExt::read_u8(&mut cur)?;
                    if byte == 0 {
                        break;
                    }
                    identifier.push(byte);
                }
                let identifier = String::from_utf8(identifier).map_err(|e| {
                    WireError::Protocol(format!("invalid OP_MSG document sequence id: {e}"))
                })?;
                let mut docs = Vec::new();
                while cur.position() < section_end {
                    docs.push(Bson::Document(Document::from_reader(&mut cur)?));
                }
                if cur.position() != section_end {
                    return Err(WireError::Protocol(
                        "OP_MSG document sequence over-read".to_string(),
                    ));
                }
                body.insert(identifier, Bson::Array(docs));
            }
            other => {
                return Err(WireError::Protocol(format!(
                    "unsupported OP_MSG section kind: {other}"
                )));
            }
        }
    }

    Ok(Some(OpMsg {
        request_id,
        response_to,
        body,
        response_protocol: ResponseProtocol::OpMsg,
    }))
}

fn read_op_query_payload(
    request_id: i32,
    response_to: i32,
    payload: Vec<u8>,
) -> WireResult<Option<OpMsg>> {
    if payload.len() < 9 {
        return Err(WireError::Protocol(format!(
            "Mongo OP_QUERY payload too short: {}",
            payload.len()
        )));
    }
    let payload_len = payload.len() as u64;
    let mut cur = Cursor::new(payload);
    let _flags = ReadBytesExt::read_i32::<LittleEndian>(&mut cur)?;

    let mut namespace = Vec::new();
    while cur.position() < payload_len {
        let byte = ReadBytesExt::read_u8(&mut cur)?;
        if byte == 0 {
            break;
        }
        namespace.push(byte);
    }
    if cur.position() >= payload_len {
        return Err(WireError::Protocol(
            "Mongo OP_QUERY namespace missing terminator".to_string(),
        ));
    }

    let namespace = String::from_utf8(namespace)
        .map_err(|e| WireError::Protocol(format!("invalid OP_QUERY namespace: {e}")))?;
    let _number_to_skip = ReadBytesExt::read_i32::<LittleEndian>(&mut cur)?;
    let _number_to_return = ReadBytesExt::read_i32::<LittleEndian>(&mut cur)?;
    let mut body = Document::from_reader(&mut cur)?;

    if !body.contains_key("$db") {
        if let Some((db, _collection)) = namespace.split_once('.') {
            body.insert("$db", db);
        }
    }

    Ok(Some(OpMsg {
        request_id,
        response_to,
        body,
        response_protocol: ResponseProtocol::OpReply,
    }))
}

pub async fn write_response<W>(
    writer: &mut W,
    protocol: ResponseProtocol,
    request_id: i32,
    response_to: i32,
    body: Document,
) -> WireResult<()>
where
    W: AsyncWrite + Unpin,
{
    match protocol {
        ResponseProtocol::OpMsg => write_op_msg(writer, request_id, response_to, body).await,
        ResponseProtocol::OpReply => write_op_reply(writer, request_id, response_to, body).await,
    }
}

async fn write_op_msg<W>(
    writer: &mut W,
    request_id: i32,
    response_to: i32,
    body: Document,
) -> WireResult<()>
where
    W: AsyncWrite + Unpin,
{
    let mut body_bytes = Vec::new();
    body.to_writer(&mut body_bytes)?;

    let message_len = 16 + 4 + 1 + body_bytes.len();
    let mut out = Vec::with_capacity(message_len);
    WriteBytesExt::write_i32::<LittleEndian>(&mut out, message_len as i32)?;
    WriteBytesExt::write_i32::<LittleEndian>(&mut out, request_id)?;
    WriteBytesExt::write_i32::<LittleEndian>(&mut out, response_to)?;
    WriteBytesExt::write_i32::<LittleEndian>(&mut out, OP_MSG)?;
    WriteBytesExt::write_i32::<LittleEndian>(&mut out, 0)?;
    WriteBytesExt::write_u8(&mut out, 0)?;
    out.extend_from_slice(&body_bytes);

    writer.write_all(&out).await?;
    writer.flush().await?;
    Ok(())
}

async fn write_op_reply<W>(
    writer: &mut W,
    request_id: i32,
    response_to: i32,
    body: Document,
) -> WireResult<()>
where
    W: AsyncWrite + Unpin,
{
    let mut body_bytes = Vec::new();
    body.to_writer(&mut body_bytes)?;

    let message_len = 16 + 4 + 8 + 4 + 4 + body_bytes.len();
    let mut out = Vec::with_capacity(message_len);
    WriteBytesExt::write_i32::<LittleEndian>(&mut out, message_len as i32)?;
    WriteBytesExt::write_i32::<LittleEndian>(&mut out, request_id)?;
    WriteBytesExt::write_i32::<LittleEndian>(&mut out, response_to)?;
    WriteBytesExt::write_i32::<LittleEndian>(&mut out, OP_REPLY)?;
    WriteBytesExt::write_i32::<LittleEndian>(&mut out, 0)?;
    WriteBytesExt::write_i64::<LittleEndian>(&mut out, 0)?;
    WriteBytesExt::write_i32::<LittleEndian>(&mut out, 0)?;
    WriteBytesExt::write_i32::<LittleEndian>(&mut out, 1)?;
    out.extend_from_slice(&body_bytes);

    writer.write_all(&out).await?;
    writer.flush().await?;
    Ok(())
}

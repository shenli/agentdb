use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;

use crate::error::{WireError, WireResult};

pub const OP_MSG: i32 = 2013;
pub const HEADER_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub message_length: i32,
    pub request_id: i32,
    pub response_to: i32,
    pub op_code: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Section {
    Body(bson::Document),
    DocumentSequence {
        identifier: String,
        documents: Vec<bson::Document>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct OpMsg {
    pub header: Header,
    pub flag_bits: u32,
    pub sections: Vec<Section>,
}

impl OpMsg {
    /// Pull the single body section if present (most commands use exactly one).
    pub fn body(&self) -> Option<&bson::Document> {
        self.sections.iter().find_map(|s| match s {
            Section::Body(d) => Some(d),
            _ => None,
        })
    }
}

pub fn decode(buf: &[u8]) -> WireResult<OpMsg> {
    if buf.len() < HEADER_LEN {
        return Err(WireError::Protocol(format!(
            "message too short: {} < {}",
            buf.len(),
            HEADER_LEN
        )));
    }
    let mut rdr = Cursor::new(buf);
    let message_length = rdr.read_i32::<LittleEndian>()?;
    let request_id = rdr.read_i32::<LittleEndian>()?;
    let response_to = rdr.read_i32::<LittleEndian>()?;
    let op_code = rdr.read_i32::<LittleEndian>()?;
    if op_code != OP_MSG {
        return Err(WireError::Protocol(format!("unsupported opcode {op_code}")));
    }
    if (message_length as usize) != buf.len() {
        return Err(WireError::Protocol(format!(
            "messageLength {} does not match buffer {}",
            message_length,
            buf.len()
        )));
    }
    let flag_bits = rdr.read_u32::<LittleEndian>()?;
    let has_checksum = (flag_bits & 0x1) != 0;
    let payload_end = buf.len() - if has_checksum { 4 } else { 0 };

    let mut sections = Vec::new();
    while (rdr.position() as usize) < payload_end {
        let kind = rdr.read_u8()?;
        match kind {
            0 => {
                let pos = rdr.position() as usize;
                let doc_len = (&buf[pos..]).read_i32::<LittleEndian>()?;
                let end = pos + doc_len as usize;
                let doc: bson::Document = bson::from_slice(&buf[pos..end])?;
                rdr.set_position(end as u64);
                sections.push(Section::Body(doc));
            }
            1 => {
                let seq_size = rdr.read_i32::<LittleEndian>()?;
                let seq_start = rdr.position() as usize;
                let seq_end = seq_start + (seq_size as usize) - 4; // size is inclusive of its own 4 bytes
                                                                   // cstring identifier
                let mut ident = Vec::new();
                loop {
                    let b = rdr.read_u8()?;
                    if b == 0 {
                        break;
                    }
                    ident.push(b);
                }
                let identifier = String::from_utf8(ident)
                    .map_err(|e| WireError::Protocol(format!("bad identifier: {e}")))?;
                let mut docs = Vec::new();
                while (rdr.position() as usize) < seq_end {
                    let pos = rdr.position() as usize;
                    let doc_len = (&buf[pos..]).read_i32::<LittleEndian>()?;
                    let end = pos + doc_len as usize;
                    let doc: bson::Document = bson::from_slice(&buf[pos..end])?;
                    rdr.set_position(end as u64);
                    docs.push(doc);
                }
                sections.push(Section::DocumentSequence {
                    identifier,
                    documents: docs,
                });
            }
            other => {
                return Err(WireError::Protocol(format!(
                    "unknown section kind 0x{other:02x}"
                )));
            }
        }
    }
    Ok(OpMsg {
        header: Header {
            message_length,
            request_id,
            response_to,
            op_code,
        },
        flag_bits,
        sections,
    })
}

/// Encode a single-body-section OP_MSG response to `to`.
pub fn encode_body_response(
    to: &mut Vec<u8>,
    request_id: i32,
    response_to: i32,
    body: &bson::Document,
) -> WireResult<()> {
    let body_bytes = bson::to_vec(body)?;
    let payload_len = 4 /* flag_bits */ + 1 /* kind */ + body_bytes.len();
    let total_len: i32 = (HEADER_LEN + payload_len) as i32;
    to.reserve(total_len as usize);
    let mut h = Vec::with_capacity(HEADER_LEN);
    h.write_i32::<LittleEndian>(total_len)?;
    h.write_i32::<LittleEndian>(request_id)?;
    h.write_i32::<LittleEndian>(response_to)?;
    h.write_i32::<LittleEndian>(OP_MSG)?;
    to.extend_from_slice(&h);
    to.write_u32::<LittleEndian>(0)?; // flag_bits
    to.write_u8(0)?; // section kind = Body
    to.extend_from_slice(&body_bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn single_body_roundtrip() {
        let request = doc! { "hello": 1, "$db": "admin" };
        let mut buf = Vec::new();
        encode_body_response(&mut buf, 42, 0, &request).unwrap();
        let decoded = decode(&buf).unwrap();
        assert_eq!(decoded.header.op_code, OP_MSG);
        assert_eq!(decoded.header.request_id, 42);
        assert_eq!(decoded.header.response_to, 0);
        assert_eq!(decoded.sections.len(), 1);
        match &decoded.sections[0] {
            Section::Body(d) => assert_eq!(d.get_i32("hello").unwrap(), 1),
            other => panic!("expected Body, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_non_opmsg() {
        let mut buf = Vec::new();
        buf.write_i32::<LittleEndian>(16).unwrap();
        buf.write_i32::<LittleEndian>(0).unwrap();
        buf.write_i32::<LittleEndian>(0).unwrap();
        buf.write_i32::<LittleEndian>(9999).unwrap(); // bad opcode
        let err = decode(&buf).unwrap_err();
        assert!(err.to_string().contains("unsupported opcode"));
    }

    #[test]
    fn decode_rejects_length_mismatch() {
        let mut buf = Vec::new();
        buf.write_i32::<LittleEndian>(999).unwrap();
        buf.write_i32::<LittleEndian>(0).unwrap();
        buf.write_i32::<LittleEndian>(0).unwrap();
        buf.write_i32::<LittleEndian>(OP_MSG).unwrap();
        let err = decode(&buf).unwrap_err();
        assert!(err.to_string().contains("messageLength"));
    }
}

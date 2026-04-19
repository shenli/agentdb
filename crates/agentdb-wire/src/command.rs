use bson::{doc, Document};

use crate::error::{WireError, WireResult};

/// Extract the command name (first field of the body) and the target database (`$db`).
pub struct CommandHead<'a> {
    pub name: &'a str,
    pub db: Option<&'a str>,
}

pub fn inspect(body: &Document) -> WireResult<CommandHead<'_>> {
    let (name, _value) = body
        .iter()
        .next()
        .ok_or_else(|| WireError::Protocol("empty command body".into()))?;
    let db = body.get_str("$db").ok();
    Ok(CommandHead {
        name: name.as_str(),
        db,
    })
}

/// Turn any internal error into an `ok: 0` response document.
pub fn error_reply(err: &WireError) -> Document {
    doc! {
        "ok": 0.0,
        "errmsg": err.to_string(),
        "code": 1,
    }
}

pub fn not_implemented(cmd: &str) -> Document {
    doc! {
        "ok": 0.0,
        "errmsg": format!("{cmd} not implemented in M3"),
        "code": 59, // CommandNotFound
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inspect_returns_command_name() {
        let body = doc! { "find": "events", "$db": "t-1", "filter": {} };
        let head = inspect(&body).unwrap();
        assert_eq!(head.name, "find");
        assert_eq!(head.db, Some("t-1"));
    }

    #[test]
    fn inspect_missing_db_is_ok() {
        let body = doc! { "hello": 1 };
        let head = inspect(&body).unwrap();
        assert_eq!(head.name, "hello");
        assert!(head.db.is_none());
    }

    #[test]
    fn error_reply_has_ok_zero_and_errmsg() {
        let err = WireError::Protocol("bad".into());
        let d = error_reply(&err);
        assert_eq!(d.get_f64("ok").unwrap(), 0.0);
        assert!(d.get_str("errmsg").unwrap().contains("bad"));
    }

    #[test]
    fn not_implemented_reply_carries_command_name() {
        let d = not_implemented("killCursors");
        assert!(d.get_str("errmsg").unwrap().contains("killCursors"));
    }
}

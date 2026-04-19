use bson::{doc, Document};

/// `startTransaction` and `commitTransaction` both return `{ok: 1}`. No
/// transaction state is tracked — the caller gets the same atomicity as
/// single-doc writes would provide on their own.
pub fn start_transaction_response() -> Document {
    doc! { "ok": 1.0 }
}

pub fn commit_transaction_response() -> Document {
    doc! { "ok": 1.0 }
}

pub fn abort_transaction_response() -> Document {
    doc! { "ok": 1.0 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_returns_ok_one() {
        assert_eq!(start_transaction_response().get_f64("ok").unwrap(), 1.0);
    }

    #[test]
    fn commit_returns_ok_one() {
        assert_eq!(commit_transaction_response().get_f64("ok").unwrap(), 1.0);
    }

    #[test]
    fn abort_returns_ok_one() {
        assert_eq!(abort_transaction_response().get_f64("ok").unwrap(), 1.0);
    }
}

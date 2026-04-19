use bson::doc;

/// Build the hello/isMaster response used by both command names.
pub fn hello_response(connection_id: i64) -> bson::Document {
    doc! {
        "isWritablePrimary": true,
        "ismaster": true,
        "helloOk": true,
        "maxBsonObjectSize": 16_777_216_i32,
        "maxMessageSizeBytes": 48_000_000_i32,
        "maxWriteBatchSize": 100_000_i32,
        "localTime": bson::DateTime::now(),
        "logicalSessionTimeoutMinutes": 30_i32,
        "connectionId": connection_id,
        "minWireVersion": 0_i32,
        "maxWireVersion": 17_i32,
        "readOnly": false,
        "saslSupportedMechs": bson::Bson::Array(vec![]),
        "ok": 1.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_advertises_writable_primary_and_ok() {
        let r = hello_response(1);
        assert!(r.get_bool("isWritablePrimary").unwrap());
        assert_eq!(r.get_f64("ok").unwrap(), 1.0);
        assert_eq!(r.get_i64("connectionId").unwrap(), 1);
    }

    #[test]
    fn hello_disables_auth_mechanisms() {
        let r = hello_response(1);
        let mechs = r.get_array("saslSupportedMechs").unwrap();
        assert!(mechs.is_empty());
    }
}

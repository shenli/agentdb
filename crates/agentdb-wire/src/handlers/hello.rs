use bson::doc;

/// Build the hello/isMaster response used by both command names.
/// `auth_enabled` advertises SCRAM-SHA-256 in `saslSupportedMechs`
/// so drivers know to run the handshake; the empty array is kept
/// when auth is off for backward compat with pre-M19 clients.
pub fn hello_response(connection_id: i64, auth_enabled: bool) -> bson::Document {
    let mechs: Vec<bson::Bson> = if auth_enabled {
        vec![bson::Bson::String("SCRAM-SHA-256".into())]
    } else {
        vec![]
    };
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
        "saslSupportedMechs": bson::Bson::Array(mechs),
        "ok": 1.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_advertises_writable_primary_and_ok() {
        let r = hello_response(1, false);
        assert!(r.get_bool("isWritablePrimary").unwrap());
        assert_eq!(r.get_f64("ok").unwrap(), 1.0);
        assert_eq!(r.get_i64("connectionId").unwrap(), 1);
    }

    #[test]
    fn hello_disables_auth_mechanisms_when_auth_off() {
        let r = hello_response(1, false);
        let mechs = r.get_array("saslSupportedMechs").unwrap();
        assert!(mechs.is_empty());
    }

    #[test]
    fn hello_advertises_scram_sha_256_when_auth_on() {
        let r = hello_response(1, true);
        let mechs = r.get_array("saslSupportedMechs").unwrap();
        let names: Vec<&str> = mechs
            .iter()
            .filter_map(|b| b.as_str())
            .collect();
        assert_eq!(names, ["SCRAM-SHA-256"]);
    }
}

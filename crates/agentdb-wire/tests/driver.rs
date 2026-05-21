use std::io::Cursor;

use bson::{doc, Document};
use mongodb::{options::ClientOptions, Client};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::test]
async fn mongodb_driver_can_insert_and_find_agentdb_events() -> anyhow::Result<()> {
    let runtime = agentdb_core::Runtime::in_memory();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server_runtime = runtime.clone();
    tokio::spawn(async move {
        let _ = agentdb_wire::server::serve(listener, server_runtime).await;
    });

    let mut options =
        ClientOptions::parse(format!("mongodb://{addr}/?directConnection=true")).await?;
    options.app_name = Some("agentdb-wire-test".to_string());
    let client = Client::with_options(options)?;
    let db = client.database("driver_test");

    let names = db.list_collection_names(None).await?;
    assert!(names.contains(&"events".to_string()));

    let events = db.collection::<Document>("events");
    events
        .insert_one(
            doc! {
                "session_alias": "mongo-driver-session",
                "kind": "message",
                "payload": {
                    "role": "user",
                    "content": "hello from mongodb driver",
                },
            },
            None,
        )
        .await?;

    let db_names = client.list_database_names(None, None).await?;
    assert!(db_names.contains(&"driver_test".to_string()));

    let mut cursor = events.find(doc! { "kind": "message" }, None).await?;
    assert!(cursor.advance().await?);
    let event: Document = cursor.deserialize_current()?;
    assert_eq!(event.get_str("kind")?, "message");
    assert_eq!(
        event.get_document("payload")?.get_str("content")?,
        "hello from mongodb driver"
    );
    assert!(event.get_str("event_id")?.starts_with("evt_"));
    assert_eq!(event.get_i64("seq")?, 0);

    let sessions = db.collection::<Document>("sessions");
    let mut cursor = sessions
        .find(doc! { "alias": "mongo-driver-session" }, None)
        .await?;
    assert!(cursor.advance().await?);
    let session: Document = cursor.deserialize_current()?;
    assert!(session.get_str("session_id")?.starts_with("ses_"));

    let records = db.collection::<Document>("records");
    records
        .insert_one(
            doc! {
                "namespace": ["memory"],
                "text": "customer prefers concise updates",
                "tags": ["preference"],
                "metadata": { "source": "driver-test" },
            },
            None,
        )
        .await?;
    let mut cursor = records.find(doc! { "namespace": ["memory"] }, None).await?;
    assert!(cursor.advance().await?);
    let record: Document = cursor.deserialize_current()?;
    assert!(record.get_str("record_id")?.starts_with("rec_"));
    assert_eq!(record.get_str("text")?, "customer prefers concise updates");

    let append_and_fetch = db
        .run_command(
            doc! {
                "agentdbAppendAndFetch": "events",
                "session_alias": "mongo-driver-session",
                "kind": "message",
                "payload": { "content": "second event" },
                "fetch_last_n": 10_i32,
            },
            None,
        )
        .await?;
    assert_eq!(append_and_fetch.get_i64("seq")?, 1);
    assert_eq!(append_and_fetch.get_array("recent")?.len(), 2);

    let count = db
        .run_command(
            doc! { "count": "events", "query": { "kind": "message" } },
            None,
        )
        .await?;
    assert_eq!(count.get_i64("n")?, 2);

    let skills = db.collection::<Document>("skills");
    skills
        .insert_one(
            doc! {
                "name": "summarize",
                "version": "1.0.0",
                "manifest": { "entrypoint": "summarize" },
            },
            None,
        )
        .await?;
    let mut cursor = skills.find(doc! { "name": "summarize" }, None).await?;
    assert!(cursor.advance().await?);
    let skill: Document = cursor.deserialize_current()?;
    assert!(skill.get_str("skill_id")?.starts_with("skl_"));

    let session: Document = sessions
        .find_one(doc! { "alias": "mongo-driver-session" }, None)
        .await?
        .expect("session exists");
    let session_id = session.get_str("session_id")?;
    let replay = db
        .run_command(
            doc! { "agentdbReplay": "sessions", "session_id": session_id },
            None,
        )
        .await?;
    assert_eq!(replay.get_document("replay")?.get_array("events")?.len(), 2);

    let context = db
        .run_command(
            doc! {
                "agentdbContext": "sessions",
                "session_id": session_id,
                "include": ["events", "policy"],
                "max_events": 10_i32,
            },
            None,
        )
        .await?;
    assert_eq!(
        context.get_document("context")?.get_array("events")?.len(),
        2
    );

    let fork = db
        .run_command(
            doc! {
                "agentdbFork": "sessions",
                "session_id": session_id,
                "at_seq": 0_i32,
                "alias": "driver-fork",
            },
            None,
        )
        .await?;
    assert!(fork
        .get_document("session")?
        .get_str("session_id")?
        .starts_with("ses_"));

    Ok(())
}

#[tokio::test]
async fn mongodb_driver_rejects_unsupported_mutations_without_side_effects() -> anyhow::Result<()> {
    let runtime = agentdb_core::Runtime::in_memory();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server_runtime = runtime.clone();
    tokio::spawn(async move {
        let _ = agentdb_wire::server::serve(listener, server_runtime).await;
    });

    let mut options =
        ClientOptions::parse(format!("mongodb://{addr}/?directConnection=true")).await?;
    options.app_name = Some("agentdb-wire-negative-test".to_string());
    let client = Client::with_options(options)?;
    let db = client.database("driver_negative");

    assert!(db
        .run_command(doc! { "create": "users" }, None)
        .await
        .is_err());
    assert!(db
        .collection::<Document>("users")
        .insert_one(doc! { "name": "not-agentdb" }, None)
        .await
        .is_err());
    assert!(db
        .run_command(
            doc! {
                "update": "events",
                "updates": [{
                    "q": { "kind": "message" },
                    "u": { "$set": { "kind": "mutated" } }
                }],
            },
            None,
        )
        .await
        .is_err());
    assert!(db
        .run_command(
            doc! {
                "delete": "events",
                "deletes": [{
                    "q": { "kind": "message" },
                    "limit": 0_i32,
                }],
            },
            None,
        )
        .await
        .is_err());

    let names = db.list_collection_names(None).await?;
    assert!(names.contains(&"events".to_string()));
    assert!(!names.contains(&"users".to_string()));

    let events = db.collection::<Document>("events");
    events
        .insert_one(
            doc! {
                "session_alias": "negative-session",
                "kind": "message",
                "payload": { "content": "still supported" },
            },
            None,
        )
        .await?;
    let count = db
        .run_command(doc! { "count": "events", "query": {} }, None)
        .await?;
    assert_eq!(count.get_i64("n")?, 1);

    Ok(())
}

#[tokio::test]
async fn legacy_op_query_hello_gets_op_reply() -> anyhow::Result<()> {
    let runtime = agentdb_core::Runtime::in_memory();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = agentdb_wire::server::serve(listener, runtime).await;
    });

    let mut stream = tokio::net::TcpStream::connect(addr).await?;
    let request_id = 41_i32;
    stream
        .write_all(&op_query_message(
            request_id,
            "admin.$cmd",
            doc! { "ismaster": 1_i32, "helloOk": true },
        )?)
        .await?;

    let mut header = [0_u8; 16];
    stream.read_exact(&mut header).await?;
    let message_len = i32::from_le_bytes(header[0..4].try_into()?);
    let response_to = i32::from_le_bytes(header[8..12].try_into()?);
    let op_code = i32::from_le_bytes(header[12..16].try_into()?);
    assert_eq!(response_to, request_id);
    assert_eq!(op_code, 1);

    let mut payload = vec![0_u8; message_len as usize - 16];
    stream.read_exact(&mut payload).await?;
    assert_eq!(i32::from_le_bytes(payload[0..4].try_into()?), 0);
    assert_eq!(i64::from_le_bytes(payload[4..12].try_into()?), 0);
    assert_eq!(i32::from_le_bytes(payload[12..16].try_into()?), 0);
    assert_eq!(i32::from_le_bytes(payload[16..20].try_into()?), 1);

    let mut doc_cursor = Cursor::new(&payload[20..]);
    let response = Document::from_reader(&mut doc_cursor)?;
    assert_eq!(response.get_f64("ok")?, 1.0);
    assert_eq!(response.get_bool("isWritablePrimary")?, true);
    assert!(response.get_i32("maxWireVersion")? >= 17);

    Ok(())
}

fn op_query_message(request_id: i32, namespace: &str, query: Document) -> anyhow::Result<Vec<u8>> {
    let mut query_bytes = Vec::new();
    query.to_writer(&mut query_bytes)?;
    let message_len = 16 + 4 + namespace.len() + 1 + 4 + 4 + query_bytes.len();

    let mut out = Vec::with_capacity(message_len);
    out.extend_from_slice(&(message_len as i32).to_le_bytes());
    out.extend_from_slice(&request_id.to_le_bytes());
    out.extend_from_slice(&0_i32.to_le_bytes());
    out.extend_from_slice(&2004_i32.to_le_bytes());
    out.extend_from_slice(&0_i32.to_le_bytes());
    out.extend_from_slice(namespace.as_bytes());
    out.push(0);
    out.extend_from_slice(&0_i32.to_le_bytes());
    out.extend_from_slice(&1_i32.to_le_bytes());
    out.extend_from_slice(&query_bytes);
    Ok(out)
}

#[tokio::test]
async fn mongodb_driver_plain_auth_accepts_valid_credentials() -> anyhow::Result<()> {
    let runtime = agentdb_core::Runtime::in_memory();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server_runtime = runtime.clone();
    tokio::spawn(async move {
        let _ = agentdb_wire::server::serve_with_auth(
            listener,
            server_runtime,
            Some(agentdb_wire::server::AuthConfig {
                username: "agentdb".to_string(),
                password: "secret".to_string(),
            }),
        )
        .await;
    });

    let bad_options = ClientOptions::parse(format!(
        "mongodb://agentdb:wrong@{addr}/?directConnection=true&authMechanism=PLAIN&authSource=%24external"
    ))
    .await?;
    let bad_client = Client::with_options(bad_options)?;
    let bad_result = bad_client
        .database("driver_auth")
        .run_command(doc! { "ping": 1_i32 }, None)
        .await;
    assert!(bad_result.is_err());

    let options = ClientOptions::parse(format!(
        "mongodb://agentdb:secret@{addr}/?directConnection=true&authMechanism=PLAIN&authSource=%24external"
    ))
    .await?;
    let client = Client::with_options(options)?;
    let db = client.database("driver_auth");

    db.run_command(doc! { "ping": 1_i32 }, None).await?;
    db.collection::<Document>("events")
        .insert_one(
            doc! {
                "session_alias": "auth-session",
                "kind": "message",
                "payload": { "content": "authenticated" },
            },
            None,
        )
        .await?;

    let mut cursor = db
        .collection::<Document>("events")
        .find(doc! { "kind": "message" }, None)
        .await?;
    assert!(cursor.advance().await?);

    Ok(())
}

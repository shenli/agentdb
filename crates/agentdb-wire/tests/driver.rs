use bson::{doc, Document};
use mongodb::{options::ClientOptions, Client};
use tokio::net::TcpListener;

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

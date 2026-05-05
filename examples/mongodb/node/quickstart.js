// AgentDB MongoDB client compatibility quickstart.
//
// Prereqs:
//   cargo run -p agentdb-server -- serve --dev --mongo-bind 127.0.0.1:7879
//   npm install mongodb

const { MongoClient } = require("mongodb");

const uri =
  process.env.AGENTDB_MONGODB_URI ??
  "mongodb://127.0.0.1:7879/?directConnection=true";
const tenant = process.env.AGENTDB_TENANT ?? "quickstart";

async function main() {
  const client = new MongoClient(uri);
  await client.connect();

  try {
    const db = client.db(tenant);
    const sessionAlias = "support-ticket-42";

    await db.collection("events").insertOne({
      session_alias: sessionAlias,
      kind: "message",
      payload: {
        role: "user",
        content: "hello from the MongoDB Node client",
      },
    });

    const appended = await db.command({
      agentdbAppendAndFetch: "events",
      session_alias: sessionAlias,
      kind: "message",
      payload: {
        role: "assistant",
        content: "hello from AgentDB",
      },
      fetch_last_n: 10,
    });

    console.log("appended", {
      event_id: appended.event_id,
      seq: appended.seq,
      recent: appended.recent.length,
    });

    const events = await db
      .collection("events")
      .find({ kind: "message" })
      .toArray();
    console.log("events", events.map((event) => event.payload));

    const session = await db
      .collection("sessions")
      .findOne({ alias: sessionAlias });
    console.log("session", {
      session_id: session.session_id,
      next_seq: session.next_seq,
    });
  } finally {
    await client.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});

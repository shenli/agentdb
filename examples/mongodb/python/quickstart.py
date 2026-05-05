"""AgentDB MongoDB client compatibility quickstart.

Prereqs:
    cargo run -p agentdb-server -- serve --dev --mongo-bind 127.0.0.1:7879
    python3 -m pip install pymongo
"""

from __future__ import annotations

import os

from pymongo import MongoClient


URI = os.environ.get(
    "AGENTDB_MONGODB_URI",
    "mongodb://127.0.0.1:7879/?directConnection=true",
)
TENANT = os.environ.get("AGENTDB_TENANT", "quickstart")


def main() -> None:
    client: MongoClient = MongoClient(URI)
    db = client[TENANT]
    session_alias = "support-ticket-42"

    db.events.insert_one(
        {
            "session_alias": session_alias,
            "kind": "message",
            "payload": {
                "role": "user",
                "content": "hello from the MongoDB Python client",
            },
        }
    )

    appended = db.command(
        {
            "agentdbAppendAndFetch": "events",
            "session_alias": session_alias,
            "kind": "message",
            "payload": {
                "role": "assistant",
                "content": "hello from AgentDB",
            },
            "fetch_last_n": 10,
        }
    )

    print(
        "appended",
        {
            "event_id": appended["event_id"],
            "seq": appended["seq"],
            "recent": len(appended["recent"]),
        },
    )

    events = list(db.events.find({"kind": "message"}))
    print("events", [event["payload"] for event in events])

    session = db.sessions.find_one({"alias": session_alias})
    print(
        "session",
        {
            "session_id": session["session_id"],
            "next_seq": session["next_seq"],
        },
    )

    client.close()


if __name__ == "__main__":
    main()

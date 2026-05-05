# MongoDB Client Examples

These examples show how to use ordinary MongoDB clients against AgentDB's
limited MongoDB wire adapter.

Start AgentDB with the MongoDB listener:

```bash
cargo run -p agentdb-server -- serve --dev --mongo-bind 127.0.0.1:7879
```

Then run one of:

```bash
node examples/mongodb/node/quickstart.js
python3 examples/mongodb/python/quickstart.py
```

Install client dependencies yourself if needed:

```bash
npm install mongodb
python3 -m pip install pymongo
```

The MongoDB listener is not a full MongoDB database. Use only the fixed AgentDB
runtime collections and extension commands described in
[`docs/reference/mongodb-compatibility.md`](../../docs/reference/mongodb-compatibility.md).

Optional basic authentication uses MongoDB `PLAIN`:

```bash
cargo run -p agentdb-server -- serve --dev \
  --mongo-bind 127.0.0.1:7879 \
  --mongo-username agentdb \
  --mongo-password secret

AGENTDB_MONGODB_URI='mongodb://agentdb:secret@127.0.0.1:7879/?directConnection=true&authMechanism=PLAIN&authSource=%24external' \
  node examples/mongodb/node/quickstart.js
```

The listener does not terminate TLS. Keep it bound to localhost or behind
separate access-control infrastructure.

# Vercel AI SDK AgentDB Template

Persistence template for Vercel AI SDK chat applications.

The template stores:

- chat messages as ordered AgentDB session events;
- stream/resume state as searchable records;
- message artifacts through AgentDB artifact references.

It is dependency-free ESM so applications can copy it into a Next.js/Vercel AI
SDK project and wire it to their existing AgentDB TypeScript client.

```js
import { Client } from "agentdb";
import { AgentDBChatStore } from "./agentdb-chat-store.js";

const client = new Client({ baseUrl: process.env.AGENTDB_URL });
const store = new AgentDBChatStore(client, {
  tenantId: "ten_...",
  agentId: "agt_...",
  sessionId: "ses_...",
});

await store.appendMessage({ id: "m1", role: "user", content: "hello" });
const messages = await store.loadMessages();
```

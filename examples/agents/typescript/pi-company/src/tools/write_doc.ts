import { Type, type Static } from "@mariozechner/pi-ai";
import type { CompanyTool } from "../agent.ts";

const params = Type.Object({
  title: Type.String(),
  body: Type.String({ description: "The full body text of the doc." }),
});

export const writeDoc: CompanyTool<typeof params> = {
  name: "write_doc",
  description:
    "Create a doc deliverable (spec, RFC, design note). Returns a doc ID.",
  parameters: params,
  handler: async (args: Static<typeof params>, ctx) => {
    const docId = `${ctx.role}-doc-${Date.now()}`;
    await ctx.memory.recordEvent({
      kind: "deliverable",
      type: "doc",
      docId,
      title: args.title,
      body: args.body,
    });
    await ctx.channels.publish("engineering", {
      kind: "doc_drafted",
      from: ctx.role,
      docId,
      title: args.title,
    });
    return { text: `wrote doc ${docId}: ${args.title}` };
  },
};

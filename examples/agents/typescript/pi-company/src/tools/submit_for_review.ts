import { Type, type Static } from "@mariozechner/pi-ai";
import type { CompanyTool } from "../agent.ts";

const params = Type.Object({
  doc_id: Type.String({ description: "The doc ID to submit for review." }),
});

export const submitForReview: CompanyTool<typeof params> = {
  name: "submit_for_review",
  description:
    "Submit a doc for review by the CTO. Posts a review_requested event.",
  parameters: params,
  handler: async (args: Static<typeof params>, ctx) => {
    await ctx.memory.recordEvent({
      kind: "tool_call",
      tool: "submit_for_review",
      docId: args.doc_id,
    });
    await ctx.channels.publish("engineering", {
      kind: "review_requested",
      from: ctx.role,
      doc_id: args.doc_id,
    });
    return { text: `submitted ${args.doc_id} for review` };
  },
};

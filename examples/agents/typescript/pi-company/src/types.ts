import type { Event as AgentDBEvent } from "agentdb";

export type AgentRole =
  | "ceo"
  | "cto"
  | "engineer"
  | "marketing"
  | "sales"
  | "contractor";

export interface AgentEvent extends AgentDBEvent {}

export interface RunIdentity {
  /** e.g. "r-1714063200" */
  runId: string;
}

export interface AgentConfig {
  role: AgentRole;
  systemPrompt: string;
  tools: string[];
  model: string;
  systemPromptSuffix?: string;
}

export type ConfigPatch = Partial<Omit<AgentConfig, "role">>;

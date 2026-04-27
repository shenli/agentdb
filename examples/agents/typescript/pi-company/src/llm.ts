import { getModel, type Api, type Model } from "@mariozechner/pi-ai";

/**
 * The four LLM providers we route to in this sample. `pi-ai` supports many
 * more — we surface only the ones the demo exercises so config errors
 * surface fast.
 */
export type Provider = "openai" | "anthropic" | "minimax" | "minimax-cn";

/**
 * Resolve a model handle for the configured provider and model id.
 *
 * Env vars consulted:
 *   AGENTDB_DEMO_PROVIDER  - one of: openai | anthropic | minimax | minimax-cn (default: anthropic)
 *   AGENTDB_DEMO_API_KEY   - the provider's API key. Falls back to provider-specific
 *                            env vars (OPENAI_API_KEY, ANTHROPIC_API_KEY, MINIMAX_API_KEY,
 *                            MINIMAX_CN_API_KEY) which pi-ai resolves automatically.
 *   AGENTDB_DEMO_MODEL     - optional override for the model id (default per provider below).
 *
 * The returned `Model<Api>` is what `pi-agent-core`'s `Agent` constructor
 * expects in `initialState.model`.
 */
export function getLLM(modelOverride?: string): Model<Api> {
  const provider = resolveProvider();

  // If AGENTDB_DEMO_API_KEY is set, populate the provider-specific env var
  // so pi-ai's auto-resolution picks it up. Don't clobber an already-set value.
  const explicitKey = process.env.AGENTDB_DEMO_API_KEY;
  if (explicitKey) {
    const targetEnv = providerEnvVar(provider);
    if (!process.env[targetEnv]) {
      process.env[targetEnv] = explicitKey;
    }
  }

  const modelId = modelOverride ?? process.env.AGENTDB_DEMO_MODEL ?? defaultModelFor(provider);

  // `getModel` is strongly typed against pi-ai's MODELS registry, so a free-form
  // string from env vars won't satisfy `keyof MODELS[provider]`. We cast at the
  // boundary; if the id is unknown, pi-ai throws at runtime with a clear error.
  return getModel(provider, modelId as never) as Model<Api>;
}

/**
 * Sanity check: throw early if no API key is available for the configured provider.
 * Called at startup so the user sees a clear error before any LLM call is attempted.
 */
export function assertApiKeyAvailable(): void {
  const provider = resolveProvider();
  const explicit = process.env.AGENTDB_DEMO_API_KEY;
  const providerKey = process.env[providerEnvVar(provider)];
  if (!explicit && !providerKey) {
    throw new Error(
      `No API key found for provider '${provider}'. Set AGENTDB_DEMO_API_KEY, or the provider-specific env var (${providerEnvVar(provider)}).`,
    );
  }
}

function resolveProvider(): Provider {
  const raw = process.env.AGENTDB_DEMO_PROVIDER ?? "anthropic";
  switch (raw) {
    case "openai":
    case "anthropic":
    case "minimax":
    case "minimax-cn":
      return raw;
    default:
      throw new Error(
        `Unsupported AGENTDB_DEMO_PROVIDER='${raw}'. Expected one of: openai, anthropic, minimax, minimax-cn.`,
      );
  }
}

function providerEnvVar(provider: Provider): string {
  switch (provider) {
    case "openai":
      return "OPENAI_API_KEY";
    case "anthropic":
      return "ANTHROPIC_API_KEY";
    case "minimax":
      return "MINIMAX_API_KEY";
    case "minimax-cn":
      return "MINIMAX_CN_API_KEY";
  }
}

function defaultModelFor(provider: Provider): string {
  switch (provider) {
    case "openai":
      return "gpt-4o-mini";
    case "anthropic":
      return "claude-haiku-4-5";
    case "minimax":
      return "MiniMax-M2.7-highspeed";
    case "minimax-cn":
      return "MiniMax-M2.7-highspeed";
  }
}

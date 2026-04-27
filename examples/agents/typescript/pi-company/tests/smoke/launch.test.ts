import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import * as path from "node:path";

const SMOKE = process.env.AGENTDB_PI_COMPANY_SMOKE === "1";
const HAS_KEY = Boolean(process.env.AGENTDB_DEMO_API_KEY);

const skipReason = !SMOKE
  ? "set AGENTDB_PI_COMPANY_SMOKE=1 to run the live-LLM smoke test"
  : !HAS_KEY
    ? "set AGENTDB_DEMO_API_KEY to run the live-LLM smoke test"
    : undefined;

test(
  "launch scenario runs end-to-end",
  { skip: skipReason, timeout: 600_000 },
  async () => {
    const here = path.dirname(fileURLToPath(import.meta.url));
    const cwd = path.resolve(here, "..", "..");

    const proc = spawn(
      "node",
      ["--experimental-strip-types", "src/index.ts", "--scenario=launch"],
      {
        cwd,
        env: process.env,
        stdio: ["ignore", "pipe", "pipe"],
      },
    );

    let stdout = "";
    proc.stdout.on("data", (b: Buffer) => { stdout += b.toString(); });
    proc.stderr.on("data", (b: Buffer) => { stdout += b.toString(); });

    const exitCode = await new Promise<number>((resolve) =>
      proc.on("exit", (c: number | null) => resolve(c ?? 1)),
    );

    assert.equal(exitCode, 0, `scenario exited with code ${exitCode}. Last 4000 chars:\n${stdout.slice(-4000)}`);

    // Sanity assertions — each role broadcast at least once.
    const lower = stdout.toLowerCase();
    assert.ok(
      /\[cto\]/i.test(stdout) || lower.includes("from: 'cto'") || lower.includes('"from":"cto"'),
      `expected CTO activity in output. Last 2000 chars:\n${stdout.slice(-2000)}`,
    );
    assert.ok(
      /\[engineer\]/i.test(stdout) || lower.includes("from: 'engineer'") || lower.includes('"from":"engineer"'),
      "expected Engineer activity",
    );
    assert.ok(
      /\[marketing\]/i.test(stdout) || lower.includes("from: 'marketing'") || lower.includes('"from":"marketing"'),
      "expected Marketing activity",
    );
    assert.ok(
      /\[sales\]/i.test(stdout) || lower.includes("from: 'sales'") || lower.includes('"from":"sales"'),
      "expected Sales activity",
    );
    // At least one fork — a contractor was hired.
    assert.ok(
      /contractor/i.test(stdout),
      "expected at least one contractor (hire_contractor fork)",
    );
  },
);

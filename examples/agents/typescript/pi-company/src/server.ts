import { spawn, type ChildProcess } from "node:child_process";
import * as net from "node:net";
import * as path from "node:path";
import * as fs from "node:fs";

async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const s = net.createServer();
    s.on("error", reject);
    s.listen(0, () => {
      const addr = s.address();
      if (typeof addr === "object" && addr !== null && "port" in addr) {
        const p = (addr as net.AddressInfo).port;
        s.close(() => resolve(p));
      } else {
        s.close();
        reject(new Error("freePort: unexpected address shape"));
      }
    });
  });
}

export interface ServerHandle {
  baseUrl: string;
  proc: ChildProcess;
  shutdown: () => void;
}

/**
 * Find the agentdb workspace root by walking up from cwd until we see
 * a Cargo.toml that mentions [workspace]. Resilient to where the demo
 * is invoked from.
 */
function findWorkspaceRoot(): string {
  let dir = process.cwd();
  for (let i = 0; i < 10; i++) {
    const cargo = path.join(dir, "Cargo.toml");
    if (fs.existsSync(cargo)) {
      const content = fs.readFileSync(cargo, "utf8");
      if (content.includes("[workspace]")) return dir;
    }
    const parent = path.dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }
  throw new Error("Could not find agentdb workspace root from " + process.cwd());
}

/**
 * Spawn `agentdb serve --dev` as a child process. Waits for /health/ready
 * before returning. SIGINT/SIGTERM/exit handlers terminate the child.
 *
 * The child uses fresh tempdirs for storage by default (--dev mode), so each
 * call gets isolated state.
 */
export async function startAgentDBServer(): Promise<ServerHandle> {
  const port = await freePort();
  const mongoPort = await freePort();
  const baseUrl = `http://127.0.0.1:${port}`;

  const repoRoot = findWorkspaceRoot();

  const proc = spawn(
    "cargo",
    ["run", "--quiet", "-p", "agentdb-server", "--", "serve", "--dev"],
    {
      cwd: repoRoot,
      env: {
        ...process.env,
        AGENTDB_ADDR: `127.0.0.1:${port}`,
        AGENTDB_MONGO_ADDR: `127.0.0.1:${mongoPort}`,
      },
      stdio: "ignore",
    },
  );

  proc.on("error", (e) => {
    console.error(`agentdb-server spawn error: ${e}`);
  });

  // Wait for /health/ready (up to 90s — cold cargo build is slow).
  const deadline = Date.now() + 90_000;
  let ready = false;
  while (Date.now() < deadline) {
    try {
      const r = await fetch(`${baseUrl}/health/ready`);
      if (r.ok) { ready = true; break; }
    } catch { /* connection refused while server is still starting */ }
    await new Promise((r) => setTimeout(r, 500));
  }
  if (!ready) {
    if (!proc.killed) proc.kill("SIGTERM");
    throw new Error(`agentdb-server did not become ready within 90s at ${baseUrl}`);
  }

  let shutdownCalled = false;
  const shutdown = () => {
    if (shutdownCalled) return;
    shutdownCalled = true;
    if (!proc.killed) proc.kill("SIGTERM");
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
  process.on("exit", shutdown);

  return { baseUrl, proc, shutdown };
}

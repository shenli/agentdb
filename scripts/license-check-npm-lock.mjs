import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const lockfiles = [
  "clients/typescript/package-lock.json",
  "adapters/vercel-ai-sdk-agentdb-template/package-lock.json",
].map((file) => path.join(root, file));

const denied = [
  /\bAGPL\b/i,
  /\bGPL\b/i,
  /\bLGPL\b/i,
  /\bSSPL\b/i,
  /\bBUSL\b/i,
  /\bCommons Clause\b/i,
  /\bElastic License\b/i,
];

let failed = false;

for (const lockfile of lockfiles) {
  if (!fs.existsSync(lockfile)) {
    continue;
  }

  const parsed = JSON.parse(fs.readFileSync(lockfile, "utf8"));
  const packages = parsed.packages ?? {};

  for (const [name, pkg] of Object.entries(packages)) {
    const license = pkg?.license;
    if (typeof license !== "string") {
      continue;
    }
    if (denied.some((pattern) => pattern.test(license))) {
      console.error(
        `error: denied npm license in ${path.relative(root, lockfile)}: ${name || "<root>"} -> ${license}`,
      );
      failed = true;
    }
  }
}

if (failed) {
  process.exit(1);
}

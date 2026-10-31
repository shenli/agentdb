#!/usr/bin/env python3
"""Exercise packaged binaries through the native SDK and a cold process restart.

Uses synthetic data and a temporary directory, never the user's agent history.
SIGKILL covers process-crash recovery, not host power loss or multi-node HA.
"""

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "clients/python"))
from agentdb import Client  # noqa: E402


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin", type=Path, default=ROOT / "target/debug/agentdb")
    parser.add_argument("--ctl", type=Path, default=ROOT / "target/debug/agentdbctl")
    args = parser.parse_args()
    server_bin, ctl_bin = args.bin.resolve(), args.ctl.resolve()
    for binary in (server_bin, ctl_bin):
        subprocess.run([str(binary), "--version"], check=True, timeout=10)

    with tempfile.TemporaryDirectory(prefix="agentdb-release-") as tmp:
        work = Path(tmp)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        config = work / "agentdb.toml"
        config.write_text(
            f'[server]\nbind = "127.0.0.1:{port}"\n[storage]\n'
            f'data_dir = {json.dumps(str(work / "data"))}\n'
            f'content_dir = {json.dumps(str(work / "content"))}\n'
        )
        # Exercise defaults independent of developer overrides or credentials.
        env = {k: v for k, v in os.environ.items() if not k.startswith("AGENTDB_")}
        env["RUST_LOG"] = "warn"
        client = Client(f"http://127.0.0.1:{port}", timeout=3)
        proc = None

        def start(log):
            nonlocal proc
            proc = subprocess.Popen(
                [str(server_bin), "serve", "--config", str(config)],
                cwd=work, env=env, stdout=log, stderr=log,
            )
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                require(proc.poll() is None, "server exited before readiness")
                try:
                    if client.health_ready():
                        return
                except Exception:
                    pass  # Connection refusal is expected while binding.
                time.sleep(0.1)
            raise RuntimeError("server did not become ready within 30 seconds")

        with (work / "server.log").open("w+") as log:
            try:
                start(log)
                session = client.open_session("release-smoke", "agent", "original")
                artifact_bytes = b"release smoke artifact\n"
                artifact = client.put_artifact(session.tenant_id, artifact_bytes,
                                               content_type="text/plain")
                payloads = [{"text": "before crash", "index": i} for i in range(3)]
                seqs = []
                for i, payload in enumerate(payloads):
                    result = session.append(
                        payload, idempotency_key=f"release-{i}", fetch_last_n=10,
                        artifact_refs=[{"artifact_id": artifact["artifact_id"]}],
                    )
                    seqs.append(result.seq)
                    require(result.recent[-1].payload == payload, "append-and-fetch mismatch")
                require(seqs == list(range(seqs[0], seqs[0] + 3)), "noncontiguous append sequence")
                proc.kill()
                proc.wait(timeout=10)
                start(log)
                reopened = client.open_session("release-smoke", "agent", "original")
                require(reopened.id == session.id, "session identity changed after restart")
                replay = reopened.replay()
                require([e.seq for e in replay] == seqs, "acknowledged sequence lost after crash")
                require([e.payload for e in replay] == payloads, "replay payload changed after crash")
                require(client.get_artifact_blob(session.tenant_id, artifact["artifact_id"])
                        == artifact_bytes, "artifact changed after crash")
                retried = reopened.append(
                    payloads[-1], idempotency_key="release-2",
                    artifact_refs=[{"artifact_id": artifact["artifact_id"]}],
                )
                require(retried.seq == seqs[-1], "idempotent retry duplicated an event")
                child = reopened.fork(alias="branch")
                require([e.payload for e in child.replay()] == payloads, "fork did not inherit tip")
                branch_payload = {"text": "alternate timeline"}
                child.append(branch_payload)
                require([e.payload for e in child.replay()] == payloads + [branch_payload],
                        "fork replay lost inherited or child history")
                require([e.payload for e in reopened.replay()] == payloads,
                        "child append mutated parent history")
                print("PASS: release recovery journey (append, SIGKILL, reopen, artifact, retry, replay, fork)")
                print("Coverage: one persistent local node, default settings; excludes host power loss and cluster HA.")
            except Exception:
                log.flush()
                log.seek(0)
                print(log.read()[-8000:], file=sys.stderr)
                raise
            finally:
                if proc is not None and proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait(timeout=10)


if __name__ == "__main__":
    main()

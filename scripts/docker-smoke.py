#!/usr/bin/env python3
"""Verify both packaged CLIs and authenticated persistent container startup."""

import argparse
import secrets
from pathlib import Path
import subprocess
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "clients/python"))
from agentdb import Client, Unauthorized  # noqa: E402


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True, timeout=90).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="agentdb:local")
    args = parser.parse_args()
    print(docker("run", "--rm", args.image, "--version"))
    print(docker("run", "--rm", "--entrypoint", "/usr/local/bin/agentdbctl",
                 args.image, "--version"))
    suffix = uuid.uuid4().hex[:12]
    name, volume = f"agentdb-smoke-{suffix}", f"agentdb-smoke-data-{suffix}"
    token = secrets.token_urlsafe(32)
    docker("volume", "create", volume)
    try:
        docker("run", "-d", "--name", name, "-p", "127.0.0.1::7878",
               "-e", f"AGENTDB_API_AUTH_TOKEN={token}", "-v", f"{volume}:/data", args.image)

        def ready():
            # Docker may allocate a new published port when restarting.
            port = docker("port", name, "7878/tcp").splitlines()[0].rsplit(":", 1)[1]
            client = Client(f"http://127.0.0.1:{port}", token=token, timeout=3)
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                try:
                    if client.health_ready():
                        return client
                except Exception:
                    pass
                time.sleep(0.2)
            raise RuntimeError("container did not become ready within 30 seconds")

        client = ready()
        try:
            Client(client.base_url, token="invalid-release-smoke-token", timeout=3).list_tenants()
        except Unauthorized:
            pass
        else:
            raise RuntimeError("container accepted an invalid bearer token")
        session = client.open_session("container-smoke", "agent", "persistent")
        payload = {"text": "survive container restart"}
        result = session.append(payload)
        docker("restart", name)
        client = ready()
        events = client.session(session.tenant_id, session.id, agent_id=session.agent_id).replay()
        if len(events) != 1 or events[0].seq != result.seq or events[0].payload != payload:
            raise RuntimeError("acknowledged event did not survive container restart")
        print("PASS: container CLIs, authenticated startup, append, restart, exact replay")
    except Exception:
        subprocess.run(["docker", "logs", "--tail", "40", name], timeout=10)
        raise
    finally:
        subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL, timeout=30)
        subprocess.run(["docker", "volume", "rm", volume], stdout=subprocess.DEVNULL, timeout=30)


if __name__ == "__main__":
    main()

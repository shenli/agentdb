#!/usr/bin/env bash
set -euo pipefail

# Opt-in end-to-end smoke for the SlateDB + S3 (MinIO) deployment shape.
# Spins up MinIO in docker, runs agentdb-server with AGENTDB_KV_BACKEND=slatedb
# and AGENTDB_OBJECT_STORE=s3, appends an event, waits for the cold-tier
# sweep, and asserts that at least one segment landed in the bucket under
# the boxes/ prefix. Not part of CI: requires docker + aws-cli locally.

command -v docker >/dev/null || { echo "[smoke-s3] docker required"; exit 1; }
command -v aws >/dev/null || { echo "[smoke-s3] aws cli required"; exit 1; }

PORT_HTTP=${PORT_HTTP:-7878}
PORT_MINIO=${PORT_MINIO:-9000}
MINIO_CONTAINER="agentdb-minio-smoke"
BUCKET="agentdb-test"
ENDPOINT="http://127.0.0.1:${PORT_MINIO}"
ACCESS_KEY="agentdbsmokeaccess"
SECRET_KEY="agentdbsmokesecret"
SERVER_PID=""

cleanup() {
    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    docker rm -f "$MINIO_CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "[smoke-s3] starting MinIO container..."
docker rm -f "$MINIO_CONTAINER" >/dev/null 2>&1 || true
docker run -d --rm \
    --name "$MINIO_CONTAINER" \
    -p "${PORT_MINIO}:9000" \
    -e "MINIO_ROOT_USER=${ACCESS_KEY}" \
    -e "MINIO_ROOT_PASSWORD=${SECRET_KEY}" \
    minio/minio server /data >/dev/null

echo "[smoke-s3] waiting for MinIO to become ready..."
READY=0
for _ in $(seq 1 60); do
    if curl -sf "${ENDPOINT}/minio/health/ready" >/dev/null; then
        READY=1
        break
    fi
    sleep 0.5
done
if [[ "$READY" -ne 1 ]]; then
    echo "[smoke-s3] MinIO did not become ready"
    docker logs "$MINIO_CONTAINER" 2>&1 | tail -30
    exit 1
fi

echo "[smoke-s3] creating bucket s3://${BUCKET}..."
AWS_ACCESS_KEY_ID="$ACCESS_KEY" \
AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
AWS_DEFAULT_REGION="us-east-1" \
aws --endpoint-url="$ENDPOINT" s3 mb "s3://${BUCKET}" >/dev/null

echo "[smoke-s3] starting agentdb-server (SlateDB + S3/MinIO)..."
AGENTDB_KV_BACKEND=slatedb \
AGENTDB_OBJECT_STORE=s3 \
AGENTDB_S3_BUCKET="$BUCKET" \
AGENTDB_S3_REGION=us-east-1 \
AGENTDB_S3_ENDPOINT="$ENDPOINT" \
AGENTDB_ADDR="127.0.0.1:${PORT_HTTP}" \
AGENTDB_T_COLD_SECS=2 \
AGENTDB_SEGMENT_GC_TICK_SECS=2 \
AGENTDB_SEGMENT_GC_GRACE_SECS=2 \
AWS_ACCESS_KEY_ID="$ACCESS_KEY" \
AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
AWS_DEFAULT_REGION="us-east-1" \
    cargo run --release -p agentdb-server >/tmp/agentdb-smoke-s3.log 2>&1 &
SERVER_PID=$!

echo "[smoke-s3] waiting for HTTP to come up on ${PORT_HTTP}..."
UP=0
for _ in $(seq 1 180); do
    if curl -sf "http://127.0.0.1:${PORT_HTTP}/api/admin/boxes" >/dev/null 2>&1; then
        UP=1
        break
    fi
    sleep 1
done
if [[ "$UP" -ne 1 ]]; then
    echo "[smoke-s3] server failed to come up"
    tail -50 /tmp/agentdb-smoke-s3.log || true
    exit 1
fi

echo "[smoke-s3] POST append_and_fetch..."
RESP=$(curl -s -X POST "http://127.0.0.1:${PORT_HTTP}/api/tenants/t-s3/sessions/s-s3/append_and_fetch" \
    -H 'Content-Type: application/json' \
    -d '{"event":{"text":"hello-from-s3"},"fetch_last_n":5}')
if ! echo "$RESP" | grep -q '"seq":1'; then
    echo "[smoke-s3] append_and_fetch failed: $RESP"
    tail -50 /tmp/agentdb-smoke-s3.log || true
    exit 1
fi

# Dashboard sanity-check, same as smoke.sh
status=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:${PORT_HTTP}/dashboard/")
if [[ "$status" != "200" ]]; then
    echo "[smoke-s3] dashboard returned $status"
    exit 1
fi

echo "[smoke-s3] waiting for cold-tier sweep (T_COLD=2s)..."
sleep 6

echo "[smoke-s3] verifying segment objects under s3://${BUCKET}/boxes/..."
SEG_COUNT=$(AWS_ACCESS_KEY_ID="$ACCESS_KEY" \
AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
AWS_DEFAULT_REGION="us-east-1" \
aws --endpoint-url="$ENDPOINT" s3 ls --recursive "s3://${BUCKET}/boxes/" 2>/dev/null | wc -l | tr -d ' ')
if [[ "${SEG_COUNT:-0}" -lt 1 ]]; then
    echo "[smoke-s3] expected at least one object under s3://${BUCKET}/boxes/, found none"
    AWS_ACCESS_KEY_ID="$ACCESS_KEY" \
    AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
    AWS_DEFAULT_REGION="us-east-1" \
    aws --endpoint-url="$ENDPOINT" s3 ls --recursive "s3://${BUCKET}/" 2>/dev/null || true
    tail -80 /tmp/agentdb-smoke-s3.log || true
    exit 1
fi
echo "[smoke-s3] found ${SEG_COUNT} object(s) under boxes/"

echo "[smoke-s3] OK"

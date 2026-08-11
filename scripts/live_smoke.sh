#!/usr/bin/env bash
set -euo pipefail

host="${1:?usage: live_smoke.sh SSH_HOST}"
smoke_id="$$"
gateway_socket="/tmp/pazuzu-smoke-${smoke_id}.sock"
control_socket="/tmp/pazuzu-smoke-${smoke_id}.ctl"
gateway_log="/tmp/pazuzu-smoke-${smoke_id}.log"
gateway_pid=""
python_bin="${PAZUZU_PYTHON:-$PWD/.venv/bin/python}"
mcp_port="${PAZUZU_MCP_PORT:-$((18000 + smoke_id % 10000))}"
mcp_log="/tmp/pazuzu-mcp-smoke-${smoke_id}.log"
mcp_pid=""
[[ -x "$python_bin" ]]

cleanup() {
  if [[ -S "$gateway_socket" ]]; then
    uv run pazuzu stop --socket "$gateway_socket" >/dev/null 2>&1 || true
  fi
  if [[ -n "$gateway_pid" ]]; then
    kill "$gateway_pid" >/dev/null 2>&1 || true
    wait "$gateway_pid" >/dev/null 2>&1 || true
  fi
  if [[ -n "$mcp_pid" ]]; then
    kill "$mcp_pid" >/dev/null 2>&1 || true
    wait "$mcp_pid" >/dev/null 2>&1 || true
  fi
  if [[ -S "$control_socket" ]]; then
    ssh -S "$control_socket" -O exit "$host" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

start_gateway() {
  "$python_bin" -m pazuzu.cli serve \
    --host "$host" \
    --socket "$gateway_socket" \
    --control-path "$control_socket" \
    --probe-interval 10 >>"$gateway_log" 2>&1 &
  gateway_pid=$!
  for _ in {1..300}; do
    if [[ -S "$gateway_socket" ]] &&
      uv run pazuzu status --socket "$gateway_socket" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "gateway did not become ready; log: $gateway_log" >&2
  return 1
}

start_gateway
echo "--- initial real-session health ---"
uv run pazuzu status --socket "$gateway_socket" --probe
echo "--- remote identity ---"
uv run pazuzu exec --socket "$gateway_socket" --retry-safe -- hostname

echo "--- force private master exit, then recover ---"
ssh -S "$control_socket" -O exit "$host" >/dev/null 2>&1 || true
uv run pazuzu exec --socket "$gateway_socket" --retry-safe -- hostname
uv run pazuzu status --socket "$gateway_socket" --probe

master_before="$(ssh -S "$control_socket" -O check "$host" 2>&1 || true)"
echo "master before gateway crash: $master_before"
kill -KILL "$gateway_pid"
wait "$gateway_pid" >/dev/null 2>&1 || true
gateway_pid=""

start_gateway
master_after="$(ssh -S "$control_socket" -O check "$host" 2>&1 || true)"
echo "master after gateway restart: $master_after"
if [[ "$master_before" != "$master_after" ]]; then
  echo "gateway restart replaced a healthy master" >&2
  exit 1
fi
uv run pazuzu exec --socket "$gateway_socket" --retry-safe -- hostname
uv run pazuzu status --socket "$gateway_socket" --probe

echo "--- Streamable HTTP MCP ---"
"$python_bin" -m pazuzu.mcp_server \
  --socket "$gateway_socket" \
  --transport streamable-http \
  --port "$mcp_port" >"$mcp_log" 2>&1 &
mcp_pid=$!
for _ in {1..200}; do
  if curl -fsS "http://127.0.0.1:${mcp_port}/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.05
done
curl -fsS "http://127.0.0.1:${mcp_port}/health"
"$python_bin" scripts/mcp_probe.py "http://127.0.0.1:${mcp_port}/mcp"
kill "$mcp_pid"
wait "$mcp_pid" || true
mcp_pid=""

echo "--- graceful shutdown ---"
uv run pazuzu stop --socket "$gateway_socket"
wait "$gateway_pid"
gateway_pid=""
[[ ! -e "$control_socket" ]]
echo "Pazuzu live smoke test passed"

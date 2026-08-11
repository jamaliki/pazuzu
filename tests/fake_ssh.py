#!/usr/bin/env python3
"""Small OpenSSH stand-in used by Pazuzu's failure tests."""

from __future__ import annotations

import fcntl
import json
import os
import signal
import socket
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

STATE_FILE = Path(os.environ["PAZUZU_FAKE_SSH_STATE"])
LOCK_FILE = STATE_FILE.with_suffix(".lock")


def update_state(change: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    with LOCK_FILE.open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        state = json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {}
        change(state)
        temporary = STATE_FILE.with_suffix(f".{os.getpid()}.tmp")
        temporary.write_text(json.dumps(state))
        os.replace(temporary, STATE_FILE)
        return state


def read_state() -> dict[str, Any]:
    return update_state(lambda state: None)


def option(flag: str) -> str | None:
    try:
        return sys.argv[sys.argv.index(flag) + 1]
    except (ValueError, IndexError):
        return None


def ssh_option(name: str) -> str | None:
    arguments = sys.argv[1:]
    for index, value in enumerate(arguments[:-1]):
        if value == "-o" and arguments[index + 1].lower().startswith(name.lower() + "="):
            return arguments[index + 1].split("=", 1)[1]
    return None


def process_alive(pid: Any) -> bool:
    try:
        os.kill(int(pid), 0)
    except (OSError, TypeError, ValueError):
        return False
    return True


def run_control(control_path: Path, operation: str) -> int:
    state = read_state()
    pid = state.get("master_pid")
    if operation == "check":
        if not control_path.exists() or not process_alive(pid):
            return 255
        client = socket.socket(socket.AF_UNIX)
        try:
            client.connect(str(control_path))
        except OSError:
            return 255
        finally:
            client.close()
        return 0
    if operation == "exit" and process_alive(pid):
        os.kill(int(pid), signal.SIGTERM)
        for _ in range(100):
            if not process_alive(pid):
                break
            time.sleep(0.01)
        return 0
    return 255


def run_master(control_path: Path) -> int:
    state = read_state()
    if state.get("auth_required"):
        print("SSH provider authentication required; renew credentials", file=sys.stderr)
        return 255
    if state.get("offline"):
        print("connect to host: operation timed out", file=sys.stderr)
        return 255
    control_path.parent.mkdir(parents=True, exist_ok=True)
    control_path.unlink(missing_ok=True)
    listener = socket.socket(socket.AF_UNIX)
    listener.bind(str(control_path))
    listener.listen()
    listener.settimeout(0.02)
    stopped = False

    def stop(_signum: int, _frame: Any) -> None:
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    def started(current: dict[str, Any]) -> None:
        current["master_pid"] = os.getpid()
        current["master_starts"] = int(current.get("master_starts", 0)) + 1
        current["healthy"] = True
        current.setdefault("events", []).append(["start", os.getpid()])

    update_state(started)
    while not stopped:
        try:
            connection, _ = listener.accept()
        except TimeoutError:
            continue
        connection.close()
    listener.close()
    control_path.unlink(missing_ok=True)
    def stopped(current: dict[str, Any]) -> None:
        current["master_pid"] = None
        current.setdefault("events", []).append(["stop", os.getpid()])

    update_state(stopped)
    return 0


def run_session(command: str) -> int:
    state = read_state()
    update_state(
        lambda current: current.setdefault("events", []).append(
            ["session", command, bool(state.get("healthy", False))]
        )
    )
    if not state.get("healthy", False):
        print("mux_client_request_session: master session failed", file=sys.stderr)
        return 255
    if command != "true" and state.get("fail_next"):
        def fail(current: dict[str, Any]) -> None:
            current["fail_next"] = False
            current["healthy"] = False

        update_state(fail)
        print("broken pipe", file=sys.stderr)
        return 255
    if command == "exit-255":
        print("remote program chose 255", file=sys.stderr)
        return 255
    if command == "sleep":
        update_state(lambda current: current.update(session_pid=os.getpid()))
        time.sleep(60)
        return 0
    if command == "cat":
        sys.stdout.buffer.write(sys.stdin.buffer.read())
        return 0
    if command == "python3 -":
        sys.stdout.buffer.write(sys.stdin.buffer.read())
        return 0
    if command.startswith("mkdir -p -- ") and command.endswith(" && sbatch --parsable"):
        script = sys.stdin.buffer.read().decode()

        def submit(current: dict[str, Any]) -> None:
            current["last_sbatch_script"] = script
            current["slurm_job_id"] = str(current.get("slurm_job_id", "12345"))
            current["slurm_state"] = "RUNNING"

        submitted = update_state(submit)
        print(f"{submitted['slurm_job_id']};fake-cluster")
        return 0
    if command.startswith("squeue -h -j "):
        current = read_state()
        if current.get("slurm_state") in {"PENDING", "RUNNING"}:
            print(
                f"{current['slurm_job_id']}|{current['slurm_state']}|"
                "00:01|02:00:00|gpu-1"
            )
            return 0
        print("slurm_load_jobs error: Invalid job id specified", file=sys.stderr)
        return 1
    if command.startswith("sacct -n -P -X -j "):
        current = read_state()
        if current.get("slurm_state"):
            print(f"{current['slurm_job_id']}|{current['slurm_state']}|00:02|0:0")
        return 0
    if command.startswith("scancel -- "):
        update_state(lambda current: current.update(slurm_state="CANCELLED"))
        return 0
    if command == "large-output":
        sys.stdout.write("x" * 1024)
        return 0
    if command == "large-gateway-output":
        sys.stdout.write("x" * (128 * 1024))
        return 0
    print(f"ran:{command}")
    return 0


def main() -> int:
    control = option("-S") or ssh_option("ControlPath")
    if not control:
        print("fake ssh requires a control path", file=sys.stderr)
        return 64
    control_path = Path(control)
    operation = option("-O")
    if operation:
        return run_control(control_path, operation)
    if "-N" in sys.argv:
        return run_master(control_path)
    return run_session(sys.argv[-1])


if __name__ == "__main__":
    raise SystemExit(main())

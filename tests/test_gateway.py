from __future__ import annotations

import asyncio
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pazuzu import PazuzuClient, SlurmJob, SlurmResources
from pazuzu.errors import PazuzuError
from pazuzu.gateway import GatewayServer, call_gateway, execute_via_gateway
from pazuzu.ssh import OpenSshSupervisor, SshSettings


def process_alive(pid: object) -> bool:
    try:
        os.kill(int(pid), 0)
    except (OSError, TypeError, ValueError):
        return False
    return True


class GatewayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.state_path = self.root / "state.json"
        self.socket_path = self.root / "gateway.sock"
        self.environment = mock.patch.dict(
            os.environ, {"PAZUZU_FAKE_SSH_STATE": str(self.state_path)}
        )
        self.environment.start()
        settings = SshSettings(
            host="test-host",
            control_path=self.root / "ssh.ctl",
            ssh_binary=str(Path(__file__).with_name("fake_ssh.py")),
            connect_timeout=1.0,
            probe_timeout=2.0,
            probe_interval=60.0,
            connection_attempts=1,
        )
        self.server = GatewayServer(OpenSshSupervisor(settings), self.socket_path)
        self.server_task = asyncio.create_task(self.server.serve())
        for _ in range(100):
            if self.socket_path.exists():
                break
            await asyncio.sleep(0.01)

    async def asyncTearDown(self) -> None:
        self.server.stop_event.set()
        await self.server_task
        self.environment.stop()
        self.temporary.cleanup()

    def state(self) -> dict[str, object]:
        return json.loads(self.state_path.read_text())

    def update_state(self, **values: object) -> None:
        current = self.state() if self.state_path.exists() else {}
        current.update(values)
        temporary = self.state_path.with_suffix(".test.tmp")
        temporary.write_text(json.dumps(current))
        os.replace(temporary, self.state_path)

    async def test_independent_clients_share_one_master(self) -> None:
        first, second = await asyncio.gather(
            execute_via_gateway(self.socket_path, "one"),
            execute_via_gateway(self.socket_path, "two"),
        )
        status = await call_gateway(self.socket_path, "status", {"probe": True})

        self.assertEqual("ran:one\n", first["stdout"])
        self.assertEqual("ran:two\n", second["stdout"])
        self.assertEqual(1, self.state()["master_starts"])
        self.assertEqual("connected", status["connection"])
        self.assertTrue(status["session_healthy"])

    async def test_client_accepts_large_valid_gateway_responses(self) -> None:
        result = await execute_via_gateway(self.socket_path, "large-gateway-output")

        self.assertEqual(128 * 1024, len(result["stdout"]))
        self.assertFalse(result["stdout_truncated"])

    async def test_disconnect_cancels_only_the_abandoned_channel(self) -> None:
        reader, writer = await asyncio.open_unix_connection(self.socket_path)
        request = {
            "v": 1,
            "id": "abandoned",
            "op": "execute",
            "params": {
                "command": "sleep",
                "stdin_base64": "",
                "timeout_seconds": 60,
                "retry_safe": False,
            },
        }
        writer.write(json.dumps(request).encode() + b"\n")
        await writer.drain()
        del reader
        session_pid = None
        for _ in range(1000):
            if self.state_path.exists():
                session_pid = self.state().get("session_pid")
                if session_pid:
                    break
            await asyncio.sleep(0.01)
        self.assertTrue(process_alive(session_pid), self.state())
        writer.close()
        await writer.wait_closed()
        for _ in range(200):
            if not process_alive(session_pid):
                break
            await asyncio.sleep(0.01)

        self.assertFalse(process_alive(session_pid))
        recovered = await execute_via_gateway(self.socket_path, "after-cancel")
        self.assertEqual("ran:after-cancel\n", recovered["stdout"])
        self.assertEqual(1, self.state()["master_starts"])

    async def test_auth_expiry_keeps_gateway_alive_and_recovers_after_login(self) -> None:
        await execute_via_gateway(self.socket_path, "before-expiry")
        self.update_state(healthy=False, auth_required=True)

        with self.assertRaisesRegex(PazuzuError, "authentication required"):
            await execute_via_gateway(self.socket_path, "during-expiry")
        unavailable = await call_gateway(self.socket_path, "status")

        self.assertEqual("authentication_required", unavailable["connection"])
        self.assertEqual("running", unavailable["gateway"])
        self.assertIn("authentication required", unavailable["last_error"])

        self.update_state(auth_required=False)
        connected = await call_gateway(self.socket_path, "reconnect")
        recovered = await execute_via_gateway(self.socket_path, "after-login")

        self.assertEqual("connected", connected["connection"])
        self.assertEqual("ran:after-login\n", recovered["stdout"])

    async def test_public_client_encodes_common_run_patterns(self) -> None:
        client = PazuzuClient(self.socket_path)

        echoed = await client.run("cat", stdin="hello")
        scripted = await client.run_script("python3", "print('hello')\n")
        queue = await client.slurm_queue()
        job = await client.inspect_slurm_job("12345")
        tail = await client.tail("/remote/log file.txt", lines=20)

        self.assertEqual("hello", echoed.stdout)
        self.assertEqual("print('hello')\n", scripted.stdout)
        self.assertIn("squeue -u", queue.stdout)
        self.assertEqual("ran:scontrol show job -o 12345\n", job.stdout)
        self.assertEqual("ran:tail -n 20 -- '/remote/log file.txt'\n", tail.stdout)

    async def test_public_client_replays_only_explicit_reads(self) -> None:
        client = PazuzuClient(self.socket_path)
        await client.run("connect")

        self.update_state(fail_next=True)
        with self.assertRaisesRegex(PazuzuError, "not replayed"):
            await client.run("mutating-command")

        self.update_state(fail_next=True)
        result = await client.read("read-only-command")

        self.assertTrue(result.replayed)
        sessions = [event[1] for event in self.state()["events"] if event[0] == "session"]
        self.assertEqual(1, sessions.count("mutating-command"))
        self.assertEqual(2, sessions.count("read-only-command"))

    async def test_public_client_validates_bounded_helpers(self) -> None:
        client = PazuzuClient(self.socket_path)

        with self.assertRaisesRegex(ValueError, "ASCII decimal"):
            await client.inspect_slurm_job("job-1")
        with self.assertRaisesRegex(ValueError, "absolute POSIX"):
            await client.tail("relative.log")
        with self.assertRaisesRegex(ValueError, "between 1 and 1000"):
            await client.tail("/remote/log", lines=0)

    async def test_public_client_owns_generic_slurm_lifecycle(self) -> None:
        client = PazuzuClient(self.socket_path)
        job = SlurmJob(
            name="client-smoke",
            argv=("python3", "-m", "train", "--label", "hello world"),
            cwd="/remote/repo",
            log_dir="/remote/logs",
            resources=SlurmResources(
                memory_gb_per_node=32,
                time_limit="02:00:00",
                cpus_per_task=4,
                gpus_per_node=1,
                partition="gpu",
            ),
            environment={"RUN_MODE": "smoke test"},
        )

        handle = await client.submit_slurm(job)
        running = await client.slurm_status(handle.job_id)
        cancelled = await client.cancel_slurm(handle.job_id)
        terminal = await client.slurm_status(handle.job_id)

        self.assertEqual("12345", handle.job_id)
        self.assertEqual("/remote/logs/client-smoke-12345.out", handle.stdout_path)
        self.assertEqual("RUNNING", running.state)
        self.assertFalse(running.terminal)
        self.assertEqual(0, cancelled.exit_code)
        self.assertEqual("CANCELLED", terminal.state)
        self.assertTrue(terminal.terminal)
        script = self.state()["last_sbatch_script"]
        self.assertIn("#SBATCH --mem=32G", script)
        self.assertIn("#SBATCH --gres=gpu:1", script)
        self.assertIn("export RUN_MODE='smoke test'", script)
        self.assertIn("exec python3 -m train --label 'hello world'", script)


if __name__ == "__main__":
    unittest.main()

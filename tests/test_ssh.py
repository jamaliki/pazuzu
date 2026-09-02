from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from pazuzu.cli import _parser
from pazuzu.errors import CommandTimedOut, ConnectionUnavailable, UncertainExecution
from pazuzu.lease import SessionLeasePool
from pazuzu.process import ProcessResult
from pazuzu.ssh import OpenSshSupervisor, SshSettings, classify_connection_failure


class SupervisorTests(unittest.IsolatedAsyncioTestCase):
    def test_default_session_budget_reserves_connection_headroom(self) -> None:
        settings = SshSettings(host="test-host", control_path=Path("/tmp/pazuzu.ctl"))
        arguments = _parser().parse_args(["serve", "--host", "test-host"])

        self.assertEqual(6, settings.max_sessions)
        self.assertEqual(settings.max_sessions, arguments.max_sessions)

    def test_three_bridges_leave_one_transient_capacity_slot(self) -> None:
        directory = self.root / "sessions"
        bridge_pool = SessionLeasePool(directory, 3)
        transient_pool = SessionLeasePool(directory, 1, start_slot=3)
        leases = [bridge_pool.acquire() for _ in range(3)]
        transient = transient_pool.acquire(timeout=0.1)
        with self.assertRaises(TimeoutError):
            transient_pool.acquire(timeout=0.05)
        transient.release()
        for lease in leases:
            lease.release()

    def test_session_lease_is_released_by_a_crashed_process(self) -> None:
        directory = self.root / "sessions"
        marker = self.root / "acquired"
        child = subprocess.Popen(
            [
                sys.executable,
                "-c",
                (
                    "import pathlib, sys, time; "
                    "from pazuzu.lease import SessionLeasePool; "
                    "lease=SessionLeasePool(pathlib.Path(sys.argv[1]), 1).acquire(); "
                    "pathlib.Path(sys.argv[2]).touch(); time.sleep(60)"
                ),
                str(directory),
                str(marker),
            ]
        )
        try:
            for _ in range(100):
                if marker.exists():
                    break
                time.sleep(0.01)
            self.assertTrue(marker.exists())
            child.kill()
            child.wait(timeout=2)
            lease = SessionLeasePool(directory, 1).acquire(timeout=0.5)
            lease.release()
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()

    def test_browser_authorization_wait_is_authentication_required(self) -> None:
        self.assertEqual(
            "authentication_required",
            classify_connection_failure("Waiting on browser..."),
        )

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.state_path = self.root / "state.json"
        self.fake_ssh = Path(__file__).with_name("fake_ssh.py")
        self.environment = mock.patch.dict(
            os.environ, {"PAZUZU_FAKE_SSH_STATE": str(self.state_path)}
        )
        self.environment.start()

    async def asyncTearDown(self) -> None:
        self.environment.stop()
        self.temporary.cleanup()

    def supervisor(self, **overrides: object) -> OpenSshSupervisor:
        values: dict[str, object] = {
            "host": "test-host",
            "control_path": self.root / "ssh.ctl",
            "ssh_binary": str(self.fake_ssh),
            "connect_timeout": 1.0,
            "probe_timeout": 2.0,
            "probe_interval": 60.0,
            "connection_attempts": 1,
            "max_output_bytes": 32,
        }
        values.update(overrides)
        return OpenSshSupervisor(SshSettings(**values))  # type: ignore[arg-type]

    def update_state(self, **values: object) -> None:
        current = json.loads(self.state_path.read_text()) if self.state_path.exists() else {}
        current.update(values)
        self.state_path.write_text(json.dumps(current))

    def read_state(self) -> dict[str, object]:
        return json.loads(self.state_path.read_text())

    async def test_executes_with_stdin_and_bounds_output(self) -> None:
        supervisor = self.supervisor()
        try:
            echoed = await supervisor.execute("cat", stdin=b"hello")
            large = await supervisor.execute("large-output")
        finally:
            await supervisor.close()

        self.assertEqual("hello", echoed.stdout)
        self.assertEqual(0, echoed.exit_code)
        self.assertEqual(32, len(large.stdout))
        self.assertTrue(large.stdout_truncated)

    async def test_shell_attachment_repairs_and_reports_the_current_generation(self) -> None:
        supervisor = self.supervisor()
        try:
            first = await supervisor.shell_attachment()
            os.kill(int(self.read_state()["master_pid"]), signal.SIGKILL)
            second = await supervisor.shell_attachment()
        finally:
            await supervisor.close()

        self.assertEqual("test-host", first.host)
        self.assertEqual(self.fake_ssh, Path(first.ssh_binary))
        self.assertEqual(self.root / "ssh.ctl", first.control_path)
        self.assertEqual(1, first.generation)
        self.assertEqual(2, second.generation)
        self.assertEqual(2, self.read_state()["master_starts"])

    async def test_safe_command_repairs_and_replays_once(self) -> None:
        supervisor = self.supervisor()
        try:
            await supervisor.execute("first")
            self.update_state(fail_next=True)
            result = await supervisor.execute("read-only", retry_safe=True)
        finally:
            await supervisor.close()

        self.assertEqual("ran:read-only\n", result.stdout)
        self.assertTrue(result.replayed)
        self.assertEqual(2, result.connection_generation)
        state = self.read_state()
        self.assertEqual(2, state["master_starts"], state.get("events"))

    async def test_unsafe_command_is_not_replayed_but_connection_recovers(self) -> None:
        supervisor = self.supervisor()
        try:
            await supervisor.execute("first")
            self.update_state(fail_next=True)
            with self.assertRaisesRegex(UncertainExecution, "not replayed"):
                await supervisor.execute("sbatch job.sh")
            recovered = await supervisor.execute("after")
        finally:
            await supervisor.close()

        self.assertEqual("ran:after\n", recovered.stdout)
        self.assertEqual(2, self.read_state()["master_starts"])

    async def test_remote_exit_255_does_not_replace_healthy_master(self) -> None:
        supervisor = self.supervisor()
        try:
            await supervisor.execute("first")
            result = await supervisor.execute("exit-255")
        finally:
            await supervisor.close()

        self.assertEqual(255, result.exit_code)
        self.assertEqual(1, result.connection_generation)
        self.assertEqual(1, self.read_state()["master_starts"])

    async def test_health_probe_shares_the_command_session_budget(self) -> None:
        supervisor = self.supervisor(max_sessions=1)
        supervisor._state = "connected"
        supervisor._generation = 1
        command_started = asyncio.Event()
        release_command = asyncio.Event()
        probe_started = asyncio.Event()

        async def session(
            command: str, *, stdin: bytes = b"", timeout: float
        ) -> ProcessResult:
            del stdin, timeout
            if command == "held-command":
                command_started.set()
                await release_command.wait()
            elif command == "true":
                probe_started.set()
            return ProcessResult(0, b"", b"", False, False)

        with (
            mock.patch.object(
                supervisor.transport,
                "control_check",
                new=mock.AsyncMock(return_value=True),
            ),
            mock.patch.object(supervisor.transport, "session", side_effect=session),
        ):
            command = asyncio.create_task(supervisor.execute("held-command"))
            await command_started.wait()
            health = asyncio.create_task(supervisor.health(probe=True))
            await asyncio.sleep(0.05)
            self.assertFalse(probe_started.is_set())
            release_command.set()
            result, status = await asyncio.gather(command, health)

        self.assertEqual(0, result.exit_code)
        self.assertTrue(probe_started.is_set())
        self.assertTrue(status["session_healthy"])

    async def test_failed_probe_waits_for_active_operation_before_repair(self) -> None:
        supervisor = self.supervisor(max_sessions=2)
        supervisor._state = "connected"
        supervisor._generation = 1
        command_started = asyncio.Event()
        release_command = asyncio.Event()
        repair_started = asyncio.Event()

        async def session(command: str, *, stdin: bytes = b"", timeout: float) -> ProcessResult:
            del stdin, timeout
            if command == "held-command":
                command_started.set()
                await release_command.wait()
            else:
                repair_started.set()
                raise CommandTimedOut("probe stalled")
            return ProcessResult(0, b"", b"", False, False)

        with (
            mock.patch.object(
                supervisor.transport, "control_check", new=mock.AsyncMock(return_value=True)
            ),
            mock.patch.object(supervisor.transport, "session", side_effect=session),
            mock.patch.object(supervisor.transport, "stop_master", new=mock.AsyncMock()),
            mock.patch.object(supervisor.transport, "start_master", new=mock.AsyncMock()),
        ):
            command = asyncio.create_task(supervisor.execute("held-command"))
            await command_started.wait()
            repair = asyncio.create_task(supervisor._repair_if_broken(1))
            await asyncio.sleep(0.05)
            self.assertFalse(repair.done())
            self.assertFalse(repair_started.is_set())
            release_command.set()
            await repair_started.wait()
            await asyncio.wait_for(asyncio.gather(command, repair), 1)

    async def test_three_bridges_serialize_transient_transfer_and_health_probe(self) -> None:
        supervisor = self.supervisor()
        supervisor._state = "connected"
        supervisor._generation = 1
        bridge_leases = [SessionLeasePool(supervisor.transport.session_leases.directory, 3).acquire()
                         for _ in range(3)]
        transfer_started = asyncio.Event()
        release_transfer = asyncio.Event()
        probe_started = asyncio.Event()

        async def transfer(*_args: object, **_kwargs: object) -> ProcessResult:
            transfer_started.set()
            await release_transfer.wait()
            return ProcessResult(0, b"", b"", False, False)

        async def session(command: str, *, stdin: bytes = b"", timeout: float) -> ProcessResult:
            del stdin, timeout
            if command == "true":
                probe_started.set()
            return ProcessResult(0, b"", b"", False, False)

        try:
            with (
                mock.patch.object(
                    supervisor.transport, "control_check", new=mock.AsyncMock(return_value=True)
                ),
                mock.patch("pazuzu.supervisor.run_transfer", side_effect=transfer),
                mock.patch.object(supervisor.transport, "session", side_effect=session),
            ):
                copy = asyncio.create_task(
                    supervisor.transfer(
                        "cp", ["--", "local", ":remote"], executable="/usr/bin/scp", timeout=1
                    )
                )
                await transfer_started.wait()
                health = asyncio.create_task(supervisor.health(probe=True))
                await asyncio.sleep(0.05)
                self.assertFalse(probe_started.is_set())
                release_transfer.set()
                result, status = await asyncio.wait_for(asyncio.gather(copy, health), 1)
        finally:
            for lease in bridge_leases:
                lease.release()
            await supervisor.close()

        self.assertEqual(0, result.exit_code)
        self.assertTrue(status["session_healthy"])
        self.assertTrue(probe_started.is_set())

    async def test_attachment_replaces_a_master_whose_probe_times_out(self) -> None:
        supervisor = self.supervisor()
        supervisor._state = "connected"
        supervisor._generation = 1

        with (
            mock.patch.object(
                supervisor.transport,
                "control_check",
                new=mock.AsyncMock(return_value=True),
            ),
            mock.patch.object(
                supervisor.transport,
                "session",
                new=mock.AsyncMock(side_effect=CommandTimedOut("probe timed out")),
            ),
            mock.patch.object(
                supervisor.transport,
                "stop_master",
                new=mock.AsyncMock(),
            ),
            mock.patch.object(
                supervisor.transport,
                "start_master",
                new=mock.AsyncMock(),
            ),
        ):
            current = await supervisor.connection_attachment()

        self.assertEqual(2, current.generation)
        self.assertEqual("connected", (await supervisor.health())["connection"])

    async def test_authentication_state_survives_and_manual_reconnects(self) -> None:
        self.update_state(auth_required=True)
        supervisor = self.supervisor()
        try:
            with self.assertRaises(ConnectionUnavailable):
                await supervisor.execute("first")
            unavailable = await supervisor.health()
            self.update_state(auth_required=False)
            connected = await supervisor.reconnect()
            result = await supervisor.execute("after-login")
        finally:
            await supervisor.close()

        self.assertEqual("authentication_required", unavailable["connection"])
        self.assertIn("authentication required", unavailable["last_error"])
        self.assertEqual("connected", connected["connection"])
        self.assertEqual("ran:after-login\n", result.stdout)

    async def test_master_logs_are_preserved_per_generation(self) -> None:
        self.update_state(offline=True)
        supervisor = self.supervisor()
        try:
            with self.assertRaises(ConnectionUnavailable):
                await supervisor.execute("before-login")
            self.update_state(offline=False)
            await supervisor.reconnect()
        finally:
            await supervisor.close()

        logs = sorted(self.root.glob("ssh.ctl.g*.log"))
        self.assertGreaterEqual(len(logs), 2)
        self.assertEqual(len(logs), len({log.name for log in logs}))

    async def test_close_kills_master_waiting_for_browser_auth(self) -> None:
        self.update_state(auth_wait=True)
        supervisor = self.supervisor(connect_timeout=30.0)
        await supervisor.start()
        auth_pid = None
        for _ in range(100):
            auth_pid = self.read_state().get("auth_wait_pid")
            if auth_pid:
                break
            await asyncio.sleep(0.01)

        await supervisor.close()

        self.assertIsNotNone(auth_pid)
        with self.assertRaises(ProcessLookupError):
            os.kill(int(auth_pid), 0)

    async def test_background_maintenance_waits_for_manual_reauthentication(self) -> None:
        self.update_state(auth_required=True)
        supervisor = self.supervisor(probe_interval=0.01)
        await supervisor.start()
        try:
            with self.assertRaises(ConnectionUnavailable):
                await supervisor.execute("before-login")
            attempts = self.read_state()["master_attempts"]
            await asyncio.sleep(0.1)
            self.assertEqual(attempts, self.read_state()["master_attempts"])

            self.update_state(auth_required=False)
            connected = await supervisor.reconnect()
        finally:
            await supervisor.close()

        self.assertEqual("connected", connected["connection"])

    async def test_background_probe_repairs_a_poisoned_master(self) -> None:
        supervisor = self.supervisor(probe_interval=0.05)
        await supervisor.start()
        try:
            for _ in range(100):
                if (await supervisor.health())["connection"] == "connected":
                    break
                await asyncio.sleep(0.02)
            self.update_state(healthy=False)
            for _ in range(150):
                current = await supervisor.health()
                if (
                    self.read_state().get("master_starts") == 2
                    and current["connection"] == "connected"
                ):
                    break
                await asyncio.sleep(0.02)
            health = await supervisor.health(probe=True)
        finally:
            await supervisor.close()

        state = self.read_state()
        self.assertEqual(2, state["master_starts"], state.get("events"))
        self.assertEqual("connected", health["connection"])
        self.assertTrue(health["session_healthy"])

    async def test_master_process_death_is_recovered_on_the_next_command(self) -> None:
        supervisor = self.supervisor()
        try:
            await supervisor.execute("first")
            os.kill(int(self.read_state()["master_pid"]), signal.SIGKILL)
            for _ in range(100):
                if not (self.root / "ssh.ctl").exists():
                    break
                await asyncio.sleep(0.01)
            recovered = await supervisor.execute("after-death")
        finally:
            await supervisor.close()

        self.assertEqual("ran:after-death\n", recovered.stdout)
        self.assertEqual(2, self.read_state()["master_starts"])

    async def test_new_gateway_adopts_a_healthy_orphaned_master(self) -> None:
        first = self.supervisor()
        second = self.supervisor()
        try:
            await first.execute("before-restart")
            result = await second.execute("after-restart")
        finally:
            await second.close()
            await first.close()

        self.assertEqual("ran:after-restart\n", result.stdout)
        self.assertEqual(1, self.read_state()["master_starts"])

    async def test_background_start_adopts_a_healthy_orphaned_master(self) -> None:
        first = self.supervisor()
        second = self.supervisor()
        try:
            await first.execute("before-restart")
            await second.start()
            for _ in range(100):
                if (await second.health())["connection"] == "connected":
                    break
                await asyncio.sleep(0.02)
            result = await second.execute("after-restart")
        finally:
            await second.close()
            await first.close()

        self.assertEqual("ran:after-restart\n", result.stdout)
        self.assertEqual(1, self.read_state()["master_starts"])

    async def test_unreachable_recorded_master_is_reaped_before_replacement(self) -> None:
        first = self.supervisor()
        second = self.supervisor()
        await first.execute("before-crash")
        old_pid = int(self.read_state()["master_pid"])
        await first.detach()
        (self.root / "ssh.ctl").unlink()

        try:
            result = await second.execute("after-crash")
            await first.transport._stop_owned_master()
        finally:
            await second.close()

        self.assertEqual("ran:after-crash\n", result.stdout)
        self.assertEqual(2, self.read_state()["master_starts"])
        with self.assertRaises(ProcessLookupError):
            os.kill(old_pid, 0)

    async def test_background_retry_recovers_after_transient_outage(self) -> None:
        supervisor = self.supervisor(probe_interval=0.05)
        await supervisor.start()
        try:
            await supervisor.execute("first")
            self.update_state(healthy=False, offline=True)
            with mock.patch("pazuzu.supervisor._bounded_backoff", return_value=0.05):
                for _ in range(100):
                    if (await supervisor.health())["connection"] == "offline":
                        break
                    await asyncio.sleep(0.02)
                self.update_state(offline=False)
                for _ in range(200):
                    if (await supervisor.health())["connection"] == "connected":
                        break
                    await asyncio.sleep(0.02)
            result = await supervisor.execute("after-network")
            health = await supervisor.health()
        finally:
            await supervisor.close()

        self.assertEqual("ran:after-network\n", result.stdout)
        self.assertEqual("connected", health["connection"])


if __name__ == "__main__":
    unittest.main()

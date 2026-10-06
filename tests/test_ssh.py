from __future__ import annotations

import asyncio
import contextlib
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
from pazuzu.lease import SessionBudget, SessionLeasePool
from pazuzu.process import ProcessResult
from pazuzu.ssh import OpenSshSupervisor, SshSettings, classify_connection_failure


class SupervisorTests(unittest.IsolatedAsyncioTestCase):
    def test_default_session_budget_leaves_headroom_for_shells(self) -> None:
        settings = SshSettings(host="test-host", control_path=Path("/tmp/pazuzu.ctl"))
        arguments = _parser().parse_args(["serve", "--host", "test-host"])
        install = _parser().parse_args(["service", "install", "--host", "test-host"])

        self.assertEqual(8, settings.max_sessions)
        self.assertEqual(settings.max_sessions, arguments.max_sessions)
        self.assertEqual(settings.max_sessions, install.max_sessions)

    def test_bridges_cannot_take_the_last_operation_or_probe_slot(self) -> None:
        budget = SessionBudget(self.root / "ssh.ctl", 6)
        bridges = [budget.bridges().acquire(timeout=0.1) for _ in range(4)]
        try:
            with self.assertRaises(TimeoutError):
                budget.bridges().acquire(timeout=0.05)
            operation = budget.operations().acquire(timeout=0.1)
            probe = budget.probes().acquire(timeout=0.1)
            with self.assertRaises(TimeoutError):
                budget.operations().acquire(timeout=0.05)
            operation.release()
            probe.release()
        finally:
            for lease in bridges:
                lease.release()

    def test_a_bridge_with_another_budget_cannot_take_the_probe_slot(self) -> None:
        gateway = SessionBudget(self.root / "ssh.ctl", 6)
        operations = [gateway.operations().acquire(timeout=0.1) for _ in range(5)]
        bridge = SessionBudget(self.root / "ssh.ctl", 8).bridges().acquire(timeout=0.1)
        try:
            gateway.probes().acquire(timeout=0.1).release()
        finally:
            bridge.release()
            for lease in operations:
                lease.release()

    def test_bridges_divide_the_budget_the_gateway_published(self) -> None:
        control = self.root / "ssh.ctl"
        self.assertEqual(8, SessionBudget.published(control, 8).max_sessions)
        SessionBudget(control, 5).publish()
        self.assertEqual(5, SessionBudget.published(control, 8).max_sessions)
        (SessionBudget(control, 5).directory / "max-sessions").write_text("1\n")
        self.assertEqual(8, SessionBudget.published(control, 8).max_sessions)

    def test_bridges_need_room_beside_operations_and_probes(self) -> None:
        with self.assertRaisesRegex(ValueError, "at least 3"):
            SessionBudget(self.root / "ssh.ctl", 2).bridges()
        with self.assertRaisesRegex(ValueError, "between 2 and 64"):
            OpenSshSupervisor(
                SshSettings(host="test-host", control_path=self.root / "ssh.ctl", max_sessions=1)
            )

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

    def held_sessions(self, supervisor: OpenSshSupervisor):
        """Mock the master so named commands hold their session until released."""

        started: dict[str, asyncio.Event] = {}
        release: dict[str, asyncio.Event] = {}
        order: list[str] = []

        async def session(command: str, *, stdin: bytes = b"", timeout: float) -> ProcessResult:
            del stdin, timeout
            order.append(command)
            started.setdefault(command, asyncio.Event()).set()
            await release.setdefault(command, asyncio.Event()).wait()
            return ProcessResult(0, command.encode(), b"", False, False)

        def gate(command: str) -> tuple[asyncio.Event, asyncio.Event]:
            return (
                started.setdefault(command, asyncio.Event()),
                release.setdefault(command, asyncio.Event()),
            )

        supervisor._state = "connected"
        supervisor._generation = 1
        patches = contextlib.ExitStack()
        patches.enter_context(
            mock.patch.object(
                supervisor.transport, "control_check", new=mock.AsyncMock(return_value=True)
            )
        )
        patches.enter_context(
            mock.patch.object(supervisor.transport, "session", side_effect=session)
        )
        return patches, gate, order

    async def test_commands_and_probes_run_while_a_transfer_is_in_flight(self) -> None:
        supervisor = self.supervisor()
        bridges = [supervisor.transport.budget.bridges().acquire(timeout=0.1) for _ in range(3)]
        patches, gate, _order = self.held_sessions(supervisor)
        transfer_started = asyncio.Event()
        release_transfer = asyncio.Event()

        async def transfer(*_args: object, **_kwargs: object) -> ProcessResult:
            transfer_started.set()
            await release_transfer.wait()
            return ProcessResult(0, b"", b"", False, False)

        _, release_quick = gate("quick")
        _, release_probe = gate("true")
        release_quick.set()
        release_probe.set()
        try:
            with patches, mock.patch("pazuzu.supervisor.run_transfer", side_effect=transfer):
                copy = asyncio.create_task(
                    supervisor.transfer(
                        "cp", ["--", "local", ":remote"], executable="/usr/bin/scp", timeout=5
                    )
                )
                await transfer_started.wait()
                quick, status = await asyncio.wait_for(
                    asyncio.gather(supervisor.execute("quick"), supervisor.health(probe=True)),
                    1,
                )
                during = await supervisor.health()
                self.assertFalse(copy.done())
                release_transfer.set()
                copied = await asyncio.wait_for(copy, 1)
        finally:
            for lease in bridges:
                lease.release()
            await supervisor.close()

        self.assertEqual("quick", quick.stdout)
        self.assertTrue(status["session_healthy"])
        self.assertEqual(1, during["operations_active"])
        self.assertEqual(0, copied.exit_code)

    async def test_operations_beyond_the_budget_queue_in_arrival_order(self) -> None:
        supervisor = self.supervisor(max_sessions=3)
        patches, gate, order = self.held_sessions(supervisor)
        with patches:
            first_started, release_first = gate("first")
            second_started, release_second = gate("second")
            _, release_third = gate("third")
            _, release_fourth = gate("fourth")
            release_third.set()
            release_fourth.set()
            first = asyncio.create_task(supervisor.execute("first"))
            second = asyncio.create_task(supervisor.execute("second"))
            await asyncio.wait_for(asyncio.gather(first_started.wait(), second_started.wait()), 1)
            third = asyncio.create_task(supervisor.execute("third"))
            await asyncio.sleep(0.01)
            fourth = asyncio.create_task(supervisor.execute("fourth"))
            await asyncio.sleep(0.1)
            queued = await supervisor.health()
            self.assertEqual(["first", "second"], order)
            release_first.set()
            release_second.set()
            await asyncio.wait_for(asyncio.gather(first, second, third, fourth), 1)

        self.assertEqual((2, 2), (queued["operations_active"], queued["operations_waiting"]))
        self.assertEqual(["first", "second", "third", "fourth"], order)

    async def test_waiting_for_a_session_is_bounded_and_never_runs_the_command(self) -> None:
        supervisor = self.supervisor(max_sessions=2)
        patches, gate, order = self.held_sessions(supervisor)
        with patches:
            started, release = gate("long")
            long = asyncio.create_task(supervisor.execute("long"))
            await started.wait()
            with self.assertRaisesRegex(CommandTimedOut, "no SSH session became free"):
                await supervisor.execute("queued", timeout=0.1)
            release.set()
            await long
            health = await supervisor.health()

        self.assertEqual(["long"], order)
        self.assertEqual((0, 0), (health["operations_active"], health["operations_waiting"]))

    async def test_queued_operations_start_in_order_while_bridges_hold_slots(self) -> None:
        supervisor = self.supervisor()
        bridges = [supervisor.transport.budget.bridges().acquire(timeout=0.1) for _ in range(6)]
        patches, gate, order = self.held_sessions(supervisor)
        names = ["a", "b", "c", "d", "e"]
        try:
            with patches:
                first_started, release_first = gate("a")
                for name in names[1:]:
                    gate(name)[1].set()
                first = asyncio.create_task(supervisor.execute("a"))
                await first_started.wait()
                queued = []
                for name in names[1:]:
                    queued.append(asyncio.create_task(supervisor.execute(name)))
                    await asyncio.sleep(0.01)
                release_first.set()
                await asyncio.wait_for(asyncio.gather(first, *queued), 5)
        finally:
            for lease in bridges:
                lease.release()

        self.assertEqual(names, order)

    async def test_connection_setup_counts_against_the_deadline(self) -> None:
        supervisor = self.supervisor()
        patches, _gate, order = self.held_sessions(supervisor)

        async def slow_connect(**_kwargs: object) -> int:
            await asyncio.sleep(0.3)
            return 1

        with (
            patches,
            mock.patch.object(supervisor, "_ensure_connected", side_effect=slow_connect),
            self.assertRaisesRegex(CommandTimedOut, "before the command could start"),
        ):
            await supervisor.execute("late", timeout=0.3)

        self.assertEqual([], order)

    async def test_a_late_check_from_an_old_generation_does_not_probe(self) -> None:
        with self.mocked_master(CommandTimedOut("probe timed out")) as supervisor:
            supervisor._generation = 2
            outcomes = [
                await supervisor._check(2),
                await supervisor._check(1),
                await supervisor._check(2),
            ]
            probes = supervisor.transport.session.await_count

        self.assertEqual(["degraded", "replaced", "degraded"], outcomes)
        self.assertEqual(2, probes)
        self.assertEqual(2, supervisor._generation)

    async def test_a_failed_repair_is_not_repeated_by_concurrent_checks(self) -> None:
        failed = ProcessResult(255, b"", b"mux_client_request_session: master failed", False, False)
        with self.mocked_master([failed]) as supervisor:
            supervisor.transport.start_master.side_effect = ConnectionUnavailable(
                "Permission denied (publickey)"
            )
            outcomes = await asyncio.gather(
                *(supervisor._check(1) for _ in range(5)), return_exceptions=True
            )
            starts = supervisor.transport.start_master.await_count
            health = await supervisor.health()

        self.assertTrue(all(isinstance(item, ConnectionUnavailable) for item in outcomes))
        self.assertEqual(1, starts)
        self.assertEqual(1, health["consecutive_failures"])
        self.assertEqual("authentication_required", health["connection"])

    async def test_failed_probe_repairs_without_waiting_for_running_work(self) -> None:
        supervisor = self.supervisor()
        supervisor._state = "connected"
        supervisor._generation = 1
        command_started = asyncio.Event()
        release_command = asyncio.Event()

        async def session(command: str, *, stdin: bytes = b"", timeout: float) -> ProcessResult:
            del stdin, timeout
            if command == "held-command":
                command_started.set()
                await release_command.wait()
                return ProcessResult(0, b"", b"", False, False)
            return ProcessResult(255, b"", b"mux_client_request_session: master failed", False, False)

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
            replaced = await asyncio.wait_for(supervisor._repair_if_broken(1), 1)
            self.assertFalse(command.done())
            release_command.set()
            await asyncio.wait_for(command, 1)

        self.assertTrue(replaced)
        self.assertEqual(2, supervisor._generation)

    async def test_concurrent_checks_share_one_probe(self) -> None:
        supervisor = self.supervisor()
        patches, gate, order = self.held_sessions(supervisor)
        with patches:
            started, release = gate("true")
            checks = [asyncio.create_task(supervisor._check(1)) for _ in range(3)]
            health = asyncio.create_task(supervisor.health(probe=True))
            await started.wait()
            await asyncio.sleep(0.05)
            release.set()
            outcomes = await asyncio.wait_for(asyncio.gather(*checks), 1)
            status = await asyncio.wait_for(health, 1)

        self.assertEqual(["healthy"] * 3, outcomes)
        self.assertTrue(status["session_healthy"])
        self.assertEqual(["true"], order)

    @contextlib.contextmanager
    def mocked_master(self, session, control_check=True):
        supervisor = self.supervisor()
        supervisor._state = "connected"
        supervisor._generation = 1
        check = (
            mock.AsyncMock(side_effect=control_check)
            if isinstance(control_check, list)
            else mock.AsyncMock(return_value=control_check)
        )
        with (
            mock.patch.object(supervisor.transport, "control_check", new=check),
            mock.patch.object(
                supervisor.transport, "session", new=mock.AsyncMock(side_effect=session)
            ),
            mock.patch.object(supervisor.transport, "stop_master", new=mock.AsyncMock()),
            mock.patch.object(supervisor.transport, "start_master", new=mock.AsyncMock()),
        ):
            yield supervisor

    async def test_slow_probes_keep_a_live_master_until_consecutive_failures(self) -> None:
        with self.mocked_master(CommandTimedOut("probe timed out")) as supervisor:
            generations, failures = [], []
            for _ in range(3):
                generations.append((await supervisor.connection_attachment()).generation)
                failures.append((await supervisor.health())["probe_failures"])

        self.assertEqual([1, 1, 2], generations)
        self.assertEqual([1, 2, 0], failures)
        self.assertEqual("connected", (await supervisor.health())["connection"])

    async def test_a_successful_probe_clears_slow_probe_failures(self) -> None:
        ok = ProcessResult(0, b"", b"", False, False)
        slow = CommandTimedOut("probe timed out")
        with self.mocked_master([slow, ok, slow, ok]) as supervisor:
            for _ in range(4):
                current = await supervisor.connection_attachment()
            health = await supervisor.health(probe=False)

        self.assertEqual(1, current.generation)
        self.assertEqual(0, health["probe_failures"])
        self.assertIsNotNone(health["last_probe_seconds"])

    async def test_slow_probe_on_a_master_that_stopped_answering_replaces_it(self) -> None:
        with self.mocked_master(
            CommandTimedOut("probe timed out"), control_check=[True, False]
        ) as supervisor:
            current = await supervisor.connection_attachment()

        self.assertEqual(2, current.generation)

    async def test_refused_probe_on_a_live_master_replaces_it_at_once(self) -> None:
        refused = ProcessResult(255, b"", b"mux_client_request_session: failed", False, False)
        with self.mocked_master([refused]) as supervisor:
            current = await supervisor.connection_attachment()

        self.assertEqual(2, current.generation)

    async def test_refused_session_counts_like_a_slow_probe(self) -> None:
        refused = ProcessResult(
            255,
            b"",
            b"mux_client_request_session: session request failed: Session open refused by peer\n",
            False,
            False,
        )
        with self.mocked_master([refused] * 3) as supervisor:
            generations, probes = [], []
            for _ in range(3):
                generations.append((await supervisor.connection_attachment()).generation)
                probes.append((await supervisor.health())["last_probe"])

        self.assertEqual([1, 1, 2], generations)
        self.assertEqual(["busy", "busy", None], probes)

    async def test_new_master_survives_a_slow_first_session(self) -> None:
        self.update_state(slow_probes=1, slow_seconds=1.0)
        supervisor = self.supervisor(probe_timeout=0.3)
        try:
            result = await supervisor.execute("after-slow-login")
        finally:
            await supervisor.close()

        self.assertEqual("ran:after-slow-login\n", result.stdout)
        self.assertEqual(1, self.read_state()["master_starts"])

    async def test_slow_reconnect_is_recorded_and_maintenance_survives(self) -> None:
        self.update_state(slow_probes=100, slow_seconds=1.0)
        supervisor = self.supervisor(probe_timeout=0.2, probe_failures=2)
        await supervisor.start()
        try:
            for _ in range(200):
                health = await supervisor.health()
                if health["consecutive_failures"]:
                    break
                await asyncio.sleep(0.02)
            self.assertEqual("offline", health["connection"])
            self.assertIn("session probe failed", health["last_error"])
            self.assertFalse(supervisor.maintenance.done())

            self.update_state(slow_probes=0)
            connected = await supervisor.reconnect()
        finally:
            await supervisor.close()

        self.assertEqual("connected", connected["connection"])

    async def test_background_probe_tolerates_one_slow_session(self) -> None:
        supervisor = self.supervisor(probe_interval=0.05, probe_timeout=0.3)
        await supervisor.start()
        try:
            for _ in range(100):
                if (await supervisor.health())["connection"] == "connected":
                    break
                await asyncio.sleep(0.02)
            self.update_state(slow_probes=1, slow_seconds=1.0)
            saw_failure = False
            for _ in range(200):
                health = await supervisor.health()
                saw_failure = saw_failure or health["probe_failures"] == 1
                if saw_failure and health["probe_failures"] == 0:
                    break
                await asyncio.sleep(0.02)
        finally:
            await supervisor.close()

        self.assertTrue(saw_failure)
        self.assertEqual(0, health["probe_failures"])
        self.assertEqual(1, self.read_state()["master_starts"])

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

    async def test_reconnect_keeps_a_healthy_master_unless_forced(self) -> None:
        supervisor = self.supervisor()
        try:
            await supervisor.execute("first")
            kept = await supervisor.reconnect()
            starts_after_plain = self.read_state()["master_starts"]
            forced = await supervisor.reconnect(force=True)
        finally:
            await supervisor.close()

        self.assertEqual(1, kept["generation"])
        self.assertEqual(1, starts_after_plain)
        self.assertEqual(2, forced["generation"])
        self.assertEqual(2, self.read_state()["master_starts"])

    async def test_maintenance_does_not_repeat_a_manual_reconnect(self) -> None:
        self.update_state(offline=True)
        supervisor = self.supervisor(probe_interval=60.0)
        await supervisor.start()
        try:
            for _ in range(200):
                if (await supervisor.health())["consecutive_failures"]:
                    break
                await asyncio.sleep(0.02)
            self.update_state(offline=False)
            connected = await supervisor.reconnect()
            # Give a mistaken second reconnect by maintenance time to happen.
            for _ in range(75):
                if self.read_state()["master_starts"] > 1:
                    break
                await asyncio.sleep(0.02)
        finally:
            await supervisor.close()

        self.assertEqual("connected", connected["connection"])
        self.assertEqual(1, self.read_state()["master_starts"])

    async def test_new_gateway_adopts_a_master_through_a_slow_probe(self) -> None:
        first = self.supervisor()
        second = self.supervisor(probe_timeout=0.3)
        try:
            await first.execute("before-restart")
            self.update_state(slow_probes=1, slow_seconds=1.0)
            result = await second.execute("after-restart")
        finally:
            await second.close()
            await first.close()

        self.assertEqual("ran:after-restart\n", result.stdout)
        self.assertEqual(1, self.read_state()["master_starts"])

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

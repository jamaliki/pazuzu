"""Connection state, repair, and conservative replay policy."""

from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from .errors import CommandTimedOut, ConnectionUnavailable, PazuzuError, UncertainExecution
from .lease import SessionLeasePool
from .process import ProcessResult
from .transfer import copy_argv, rsync_argv, run_transfer
from .transport import OpenSshTransport, SshAttachment, SshSettings

AUTH_MARKERS = (
    "authentication failed",
    "authentication required",
    "certificate expired",
    "login required",
    "not logged in",
    "permission denied",
    "waiting on browser",
)


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


def _bounded_backoff(failures: int) -> float:
    """Return a quiet reconnect delay, capped at one minute."""

    return min(60.0, 5.0 * (2 ** max(0, failures - 1)))


def classify_connection_failure(detail: str) -> str:
    """Separate actionable authentication failures from ordinary outages."""

    lowered = detail.lower()
    return "authentication_required" if any(item in lowered for item in AUTH_MARKERS) else "offline"


@dataclass(frozen=True)
class CommandResult:
    """Bounded result from one independent SSH command channel."""

    exit_code: int
    stdout: str
    stderr: str
    stdout_truncated: bool
    stderr_truncated: bool
    connection_generation: int
    replayed: bool = False

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class _SessionSlots:
    """Admit gateway operations to the shared session budget in arrival order.

    The cross-process pool keeps the gateway and its bridges inside the
    budget.  Only the operation at the head of the local queue polls it, so
    operations start in the order they arrived instead of racing each other.
    """

    def __init__(self, pool: SessionLeasePool) -> None:
        self._pool = pool
        self._head = asyncio.Lock()
        self.active = 0
        self.waiting = 0

    @contextlib.asynccontextmanager
    async def hold(self, timeout: float):
        """Hold one session slot, waiting at most ``timeout`` seconds for it."""

        self.waiting += 1
        try:
            async with asyncio.timeout(timeout), self._head:
                lease = await self._pool.acquire_async()
        except TimeoutError as exc:
            raise CommandTimedOut(
                f"no SSH session became free within {timeout:g}s; "
                f"{self.active} gateway operations hold the session budget"
            ) from exc
        finally:
            self.waiting -= 1
        self.active += 1
        try:
            yield
        finally:
            self.active -= 1
            lease.release()


class OpenSshSupervisor:
    """Probe, repair, and proactively reconnect one OpenSSH transport.

    Every command and transfer runs concurrently on its own channel, limited
    only by the session budget.  The repair lock guards connect and replace
    decisions alone; it is never held across an operation or while waiting for
    a session, so a long transfer delays nothing except work queued behind a
    full budget.  Health probes use a reserved slot, and concurrent callers
    share one probe instead of each opening another session.
    """

    def __init__(self, settings: SshSettings) -> None:
        self.settings = settings
        self.transport = OpenSshTransport(settings)
        self._repair_lock = asyncio.Lock()
        self._slots = _SessionSlots(self.transport.budget.operations())
        self._probe_flight: tuple[int, asyncio.Task[str]] | None = None
        # Counts every attempt to adopt or replace a master, including failed
        # ones, so a check can tell that another caller already acted.
        self._repair_epoch = 0
        self._wake = asyncio.Event()
        self._maintainer: asyncio.Task[None] | None = None
        self._closed = False
        self._state = "offline"
        self._generation = 0
        self._connected_at: str | None = None
        self._last_success_at: str | None = None
        self._last_failure_at: str | None = None
        self._last_error: str | None = None
        self._failures = 0
        self._next_attempt = 0.0
        self._probe_failures = 0
        self._last_probe: str | None = None
        self._last_probe_seconds: float | None = None

    @property
    def maintenance(self) -> asyncio.Task[None] | None:
        """The background maintenance task, which runs until ``close``."""

        return self._maintainer

    async def start(self) -> None:
        """Start proactive connection maintenance without blocking local startup."""

        if self._maintainer is None:
            # Bridges divide the session budget the way this gateway does.
            self.transport.budget.publish()
            self._maintainer = asyncio.create_task(self._maintain(), name="pazuzu-ssh")

    async def close(self) -> None:
        """Stop maintenance and the owned master."""

        await self._stop_maintenance()
        async with self._repair_lock:
            await self.transport.stop_master()
            self._state = "stopped"

    async def detach(self) -> None:
        """Stop the gateway while preserving its authenticated master."""

        await self._stop_maintenance()
        self.transport.detach_master()
        self._state = "detached"

    async def execute(
        self,
        command: str,
        *,
        stdin: bytes = b"",
        timeout: float = 120.0,
        retry_safe: bool = False,
    ) -> CommandResult:
        """Execute once, replaying only an explicitly idempotent operation.

        ``timeout`` covers waiting for a free session, connection setup, and
        the command itself.
        """

        if not command or "\x00" in command:
            raise ValueError("remote command must be a non-empty string without NUL bytes")
        if timeout <= 0:
            raise ValueError("command timeout must be positive")
        result, generation = await self._run_session(command, stdin, timeout)
        if result.exit_code != 255:
            return self._command_result(result, generation)
        if not await self._safe_repair_after_command(generation):
            return self._command_result(result, generation)
        if not retry_safe:
            raise UncertainExecution(
                "SSH lost the command channel after the remote command may have started; "
                "the command was not replayed"
            )
        return await self._replay_once(command, stdin, timeout)

    async def reconnect(self, *, force: bool = False) -> dict[str, Any]:
        """Repair immediately, bypassing backoff.

        A connected master is probed and kept when it is healthy, so in-flight
        commands, transfers, and bridges survive.  ``force`` replaces it anyway.
        """

        self._next_attempt = 0.0
        try:
            if force:
                await self._connect(force=True)
            else:
                await self._check(await self._ensure_connected(respect_backoff=False))
        finally:
            self._wake.set()
        return await self.health(probe=False)

    async def connection_attachment(self) -> SshAttachment:
        """Return a probed snapshot for a direct client of the current master."""

        await self._ensure_connected()
        await self._repair_if_broken(self._generation)
        return self._attachment(self._generation)

    async def transfer(
        self,
        tool: str,
        arguments: list[str],
        *,
        executable: str,
        timeout: float,
    ) -> CommandResult:
        """Run one bounded transfer on its own channel within the session budget.

        Transfers are deliberately not replayed: an interrupted copy may have
        partially changed its destination.  The gateway owns the subprocess,
        so disconnecting clients and deadlines can terminate its whole process
        group instead of leaving an orphaned mux client behind.
        """

        if tool not in {"cp", "rsync"}:
            raise ValueError("transfer tool must be cp or rsync")
        if not executable or "\x00" in executable:
            raise ValueError("transfer executable must be non-empty and contain no NUL bytes")
        if timeout <= 0:
            raise ValueError("transfer timeout must be positive")
        # The command line does not depend on the generation, so reject invalid
        # arguments before waiting for a session or a connection.
        build = copy_argv if tool == "cp" else rsync_argv
        argv = build(self._attachment(max(1, self._generation)), arguments, executable=executable)
        async with self._operation(timeout) as (generation, remaining):
            result = await run_transfer(
                argv, timeout=remaining, output_limit=self.settings.max_output_bytes
            )
        if result.exit_code == 255:
            await self._safe_repair_after_command(generation)
        return self._command_result(result, generation)

    async def shell_attachment(self) -> SshAttachment:
        """Return a probed master snapshot for an interactive client."""

        return await self.connection_attachment()

    async def health(self, *, probe: bool = False) -> dict[str, Any]:
        """Return bounded local and remote connection state."""

        session_healthy: bool | None = None
        if probe and self._state == "connected":
            with contextlib.suppress(ConnectionUnavailable):
                await self._repair_if_broken(self._generation)
            session_healthy = self._state == "connected" and not self._probe_failures
        retry_in = max(0.0, self._next_attempt - time.monotonic())
        return {
            "gateway": "running" if not self._closed else "stopped",
            "connection": self._state,
            "host": self.settings.host,
            "generation": self._generation,
            "session_healthy": session_healthy,
            "connected_at": self._connected_at,
            "last_success_at": self._last_success_at,
            "last_failure_at": self._last_failure_at,
            "last_error": self._last_error,
            "consecutive_failures": self._failures,
            "last_probe": self._last_probe,
            "probe_failures": self._probe_failures,
            "last_probe_seconds": self._last_probe_seconds,
            "retry_in_seconds": round(retry_in, 1),
            "max_sessions": self.settings.max_sessions,
            "operations_active": self._slots.active,
            "operations_waiting": self._slots.waiting,
        }

    def _attachment(self, generation: int) -> SshAttachment:
        return SshAttachment(
            host=self.settings.host,
            ssh_binary=self.settings.ssh_binary,
            control_path=self.settings.control_path,
            generation=generation,
        )

    @contextlib.asynccontextmanager
    async def _operation(self, timeout: float):
        """Hold a session slot on a connected master; yield its generation and time left.

        The caller's deadline covers queueing and connection setup, matching
        the client's own deadline, so the command gets only what remains.
        """

        started = time.monotonic()
        async with self._slots.hold(timeout):
            generation = await self._ensure_connected()
            remaining = timeout - (time.monotonic() - started)
            if remaining < 0.1:
                raise CommandTimedOut(
                    f"the {timeout:g}s deadline passed before the command could start"
                )
            yield generation, remaining

    async def _run_session(
        self, command: str, stdin: bytes, timeout: float
    ) -> tuple[ProcessResult, int]:
        async with self._operation(timeout) as (generation, remaining):
            result = await self.transport.session(command, stdin=stdin, timeout=remaining)
        return result, generation

    async def _maintain(self) -> None:
        while not self._closed:
            if self._state == "authentication_required":
                await self._wake.wait()
                self._wake.clear()
                continue
            if self._state != "connected":
                delay = max(0.0, self._next_attempt - time.monotonic())
            elif self._probe_failures:
                # Confirm or clear a failed probe quickly instead of a minute later.
                delay = min(self.settings.probe_interval, 10.0)
            else:
                delay = self.settings.probe_interval
            if await self._wait_for_wake(delay):
                # A manual reconnect already acted, or the gateway is closing;
                # reschedule from the new state instead of repeating its work.
                continue
            try:
                if self._state == "connected":
                    await self._repair_if_broken(self._generation)
                else:
                    await self._connect(force=False)
            except PazuzuError:
                # Failures are recorded in the connection state; maintenance must
                # outlive every one of them.
                pass

    async def _stop_maintenance(self) -> None:
        self._closed = True
        self._wake.set()
        task, self._maintainer = self._maintainer, None
        flight, self._probe_flight = self._probe_flight, None
        for pending in (task, flight[1] if flight else None):
            if pending is not None:
                pending.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await pending

    async def _wait_for_wake(self, timeout: float) -> bool:
        if self._wake.is_set():
            self._wake.clear()
            return True
        try:
            await asyncio.wait_for(self._wake.wait(), timeout)
        except TimeoutError:
            return False
        self._wake.clear()
        return True

    async def _ensure_connected(self, *, respect_backoff: bool = True) -> int:
        if self._state == "connected" and await self.transport.control_check():
            return self._generation
        await self._connect(force=False, respect_backoff=respect_backoff)
        return self._generation

    async def _connect(self, *, force: bool, respect_backoff: bool = True) -> None:
        async with self._repair_lock:
            if not force and self._state == "connected" and await self.transport.control_check():
                return
            if not force and respect_backoff and time.monotonic() < self._next_attempt:
                raise ConnectionUnavailable(self._unavailable_message())
            if not force and self._generation == 0:
                self._repair_epoch += 1
                if await self.transport.adopt_existing():
                    self._generation += 1
                    self._record_connected()
                    return
            await self._replace_master()

    async def _replace_master(self) -> None:
        self._repair_epoch += 1
        self._state = "reconnecting"
        await self.transport.stop_master()
        errors: list[str] = []
        for attempt in range(self.settings.connection_attempts):
            try:
                await self.transport.start_master()
                self._generation += 1
                self._record_connected()
                return
            except (ConnectionUnavailable, CommandTimedOut) as exc:
                errors.append(str(exc))
                await self.transport.stop_master()
                if classify_connection_failure(str(exc)) == "authentication_required":
                    break
                if attempt + 1 < self.settings.connection_attempts:
                    await asyncio.sleep(min(2.0, 0.5 * (2**attempt)))
        detail = errors[-1] if errors else "unknown SSH startup failure"
        self._record_failure(detail)
        raise ConnectionUnavailable(self._unavailable_message())

    async def _safe_repair_after_command(self, generation: int) -> bool:
        """Whether a command's exit 255 came from the connection rather than the command."""

        try:
            return await self._check(generation) != "healthy"
        except ConnectionUnavailable as exc:
            raise UncertainExecution(
                "SSH disconnected after the remote command may have started, "
                f"and automatic reconnection failed: {exc}"
            ) from exc

    async def _repair_if_broken(self, failed_generation: int) -> bool:
        return await self._check(failed_generation) == "replaced"

    async def _check(self, failed_generation: int) -> str:
        """Probe the master: ``healthy``, ``degraded`` (kept), or ``replaced``.

        A probe that times out, or that the server refuses, while the master's
        control socket still answers usually means a busy remote host or a full
        session limit: OpenSSH keepalives already guard the encrypted
        connection, and replacing it would drop every bridge and in-flight
        command and make that same host authenticate a new one.  Such a master
        is replaced only after ``probe_failures`` consecutive failures, which
        also covers a proxy whose upstream has silently gone.  A master that
        fails the session any other way, or no longer answers, is replaced at
        once.

        The probe runs outside the repair lock in its own reserved slot, so it
        never waits for, or is mistaken for, ordinary work.  When several
        callers see the same failure, the first repairs and the rest accept its
        result: a failed repair raises instead of being repeated, leaving
        further attempts to backoff.
        """

        if failed_generation != self._generation and self._state == "connected":
            return "replaced"
        epoch = self._repair_epoch
        outcome = await self._probe() if self._state == "connected" else "failed"
        if outcome == "ok" and epoch == self._repair_epoch:
            return "healthy"
        async with self._repair_lock:
            if epoch != self._repair_epoch:
                if self._state == "connected":
                    return "replaced"
                raise ConnectionUnavailable(self._unavailable_message())
            if (
                outcome in {"slow", "busy"}
                and self._probe_failures < self.settings.probe_failures
                and await self.transport.control_check()
            ):
                return "degraded"
            if self._state != "connected" and time.monotonic() < self._next_attempt:
                raise ConnectionUnavailable(self._unavailable_message())
            await self._replace_master()
            return "replaced"

    async def _probe(self) -> str:
        """Probe the current master once, sharing an in-flight probe with concurrent callers."""

        epoch = self._repair_epoch
        flight = self._probe_flight
        if flight is None or flight[0] != epoch or flight[1].done():
            task = asyncio.create_task(self._run_probe(epoch), name="pazuzu-probe")
            flight = self._probe_flight = (epoch, task)
        # A caller that gives up must not cancel the probe other callers await.
        return await asyncio.shield(flight[1])

    async def _run_probe(self, epoch: int) -> str:
        started = time.monotonic()
        outcome = await self.transport.probe()
        # A probe that overlapped a repair says nothing about the new master.
        if epoch == self._repair_epoch:
            self._last_probe = outcome
            self._last_probe_seconds = round(time.monotonic() - started, 3)
            if outcome == "ok":
                self._probe_failures = 0
                self._last_success_at = _now()
            else:
                self._probe_failures += 1
        return outcome

    async def _replay_once(self, command: str, stdin: bytes, timeout: float) -> CommandResult:
        replay, generation = await self._run_session(command, stdin, timeout)
        if replay.exit_code == 255 and await self._check(generation) != "healthy":
            raise ConnectionUnavailable(
                "the idempotent command lost its connection again after one replay"
            )
        return self._command_result(replay, generation, replayed=True)

    def _record_connected(self) -> None:
        timestamp = _now()
        self._state = "connected"
        self._connected_at = timestamp
        self._last_success_at = timestamp
        self._last_error = None
        self._failures = 0
        self._next_attempt = 0.0
        self._probe_failures = 0
        self._last_probe = None

    def _record_failure(self, detail: str) -> None:
        self._state = classify_connection_failure(detail)
        self._last_failure_at = _now()
        self._last_error = detail[-2000:]
        self._failures += 1
        self._next_attempt = time.monotonic() + _bounded_backoff(self._failures)

    def _unavailable_message(self) -> str:
        detail = self._last_error or "SSH connection is unavailable"
        retry_in = max(0.0, self._next_attempt - time.monotonic())
        suffix = f"; automatic retry in {retry_in:.0f}s" if retry_in else ""
        if self._state == "authentication_required":
            suffix += "; reauthorise with the configured SSH provider, then run pazuzu reconnect"
        return f"{detail}{suffix}"

    @staticmethod
    def _command_result(
        result: ProcessResult, generation: int, *, replayed: bool = False
    ) -> CommandResult:
        return CommandResult(
            exit_code=result.exit_code,
            stdout=result.stdout.decode(errors="replace"),
            stderr=result.stderr.decode(errors="replace"),
            stdout_truncated=result.stdout_truncated,
            stderr_truncated=result.stderr_truncated,
            connection_generation=generation,
            replayed=replayed,
        )


__all__ = ["CommandResult", "OpenSshSupervisor", "classify_connection_failure"]

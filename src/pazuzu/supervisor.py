"""Connection state, repair, and conservative replay policy."""

from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from .errors import ConnectionUnavailable, UncertainExecution
from .process import ProcessResult
from .transport import OpenSshTransport, ShellAttachment, SshSettings

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


class OpenSshSupervisor:
    """Probe, repair, and proactively reconnect one OpenSSH transport."""

    def __init__(self, settings: SshSettings) -> None:
        self.settings = settings
        self.transport = OpenSshTransport(settings)
        self._lock = asyncio.Lock()
        self._sessions = asyncio.Semaphore(settings.max_sessions)
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

    async def start(self) -> None:
        """Start proactive connection maintenance without blocking local startup."""

        if self._maintainer is None:
            self._maintainer = asyncio.create_task(self._maintain(), name="pazuzu-ssh")

    async def close(self) -> None:
        """Stop maintenance and the owned master."""

        await self._stop_maintenance()
        async with self._lock:
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
        """Execute once, replaying only an explicitly idempotent operation."""

        if not command or "\x00" in command:
            raise ValueError("remote command must be a non-empty string without NUL bytes")
        if timeout <= 0:
            raise ValueError("command timeout must be positive")
        async with self._sessions:
            generation = await self._ensure_connected()
            result = await self.transport.session(command, stdin=stdin, timeout=timeout)
        # A failed command must release its slot before the repair probe reserves one.
        if result.exit_code != 255:
            return self._command_result(result, generation)
        repaired = await self._safe_repair_after_command(generation)
        if not repaired:
            return self._command_result(result, generation)
        if not retry_safe:
            raise UncertainExecution(
                "SSH disconnected after the remote command may have started; "
                "the connection was repaired, but the command was not replayed"
            )
        return await self._replay_once(command, stdin, timeout)

    async def reconnect(self) -> dict[str, Any]:
        """Clear backoff and immediately establish a fresh connection."""

        self._next_attempt = 0.0
        try:
            await self._connect(force=True)
        finally:
            self._wake.set()
        return await self.health(probe=False)

    async def shell_attachment(self) -> ShellAttachment:
        """Return a snapshot of the current master for an interactive client."""

        generation = await self._ensure_connected()
        return ShellAttachment(
            host=self.settings.host,
            ssh_binary=self.settings.ssh_binary,
            control_path=self.settings.control_path,
            generation=generation,
        )

    async def health(self, *, probe: bool = False) -> dict[str, Any]:
        """Return bounded local and remote connection state."""

        session_healthy: bool | None = None
        if probe and self._state == "connected":
            try:
                await self._repair_if_broken(self._generation)
            except ConnectionUnavailable:
                pass
            session_healthy = self._state == "connected"
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
            "retry_in_seconds": round(retry_in, 1),
        }

    async def _maintain(self) -> None:
        while not self._closed:
            if self._state == "authentication_required":
                await self._wake.wait()
                self._wake.clear()
                continue
            delay = self.settings.probe_interval if self._state == "connected" else max(
                0.0, self._next_attempt - time.monotonic()
            )
            forced = await self._wait_for_wake(delay)
            if self._closed:
                return
            try:
                if self._state == "connected" and not forced:
                    await self._repair_if_broken(self._generation)
                else:
                    await self._connect(force=forced)
            except ConnectionUnavailable:
                pass

    async def _stop_maintenance(self) -> None:
        self._closed = True
        self._wake.set()
        task, self._maintainer = self._maintainer, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

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

    async def _ensure_connected(self) -> int:
        if self._state == "connected" and await self.transport.control_check():
            return self._generation
        await self._connect(force=False)
        return self._generation

    async def _connect(self, *, force: bool) -> None:
        async with self._lock:
            if not force and self._state == "connected" and await self.transport.control_check():
                return
            if not force and time.monotonic() < self._next_attempt:
                raise ConnectionUnavailable(self._unavailable_message())
            if not force and self._generation == 0 and await self.transport.adopt_existing():
                self._generation += 1
                self._record_connected()
                return
            await self._replace_master()

    async def _replace_master(self) -> None:
        self._state = "reconnecting"
        await self.transport.stop_master()
        errors: list[str] = []
        for attempt in range(self.settings.connection_attempts):
            try:
                await self.transport.start_master()
                self._generation += 1
                self._record_connected()
                return
            except ConnectionUnavailable as exc:
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
        try:
            return await self._repair_if_broken(generation)
        except ConnectionUnavailable as exc:
            raise UncertainExecution(
                "SSH disconnected after the remote command may have started, "
                f"and automatic reconnection failed: {exc}"
            ) from exc

    async def _repair_if_broken(self, failed_generation: int) -> bool:
        async with self._lock:
            if failed_generation != self._generation and self._state == "connected":
                return True
            async with self._sessions:
                healthy = await self.transport.probe()
            if healthy:
                self._last_success_at = _now()
                return False
            await self._replace_master()
            return True

    async def _replay_once(self, command: str, stdin: bytes, timeout: float) -> CommandResult:
        async with self._sessions:
            generation = await self._ensure_connected()
            replay = await self.transport.session(command, stdin=stdin, timeout=timeout)
        if replay.exit_code == 255 and await self._repair_if_broken(generation):
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

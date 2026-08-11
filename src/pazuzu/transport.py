"""Own the raw OpenSSH ControlMaster and command channels."""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import CommandTimedOut, ConnectionUnavailable
from .process import ProcessResult, run_process


def _spawn_master(argv: list[str], log_handle: Any) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        argv,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=log_handle,
        start_new_session=True,
    )


@dataclass(frozen=True)
class SshSettings:
    """Configuration for one private OpenSSH master."""

    host: str
    control_path: Path
    ssh_binary: str = "/usr/bin/ssh"
    connect_timeout: float = 60.0
    probe_timeout: float = 10.0
    probe_interval: float = 60.0
    keepalive_interval: int = 15
    keepalive_count: int = 3
    connection_attempts: int = 2
    max_sessions: int = 8
    max_output_bytes: int = 1024 * 1024

    def validate(self) -> None:
        if not self.host:
            raise ValueError("SSH host must not be empty")
        if not 1 <= self.max_sessions <= 64:
            raise ValueError("max_sessions must be between 1 and 64")
        if not 1 <= self.connection_attempts <= 5:
            raise ValueError("connection_attempts must be between 1 and 5")
        if min(self.connect_timeout, self.probe_timeout, self.probe_interval) <= 0:
            raise ValueError("SSH timeouts must be positive")
        if len(os.fsencode(self.control_path)) > 100:
            raise ValueError("control socket path is too long; choose a path below 100 bytes")


class OpenSshTransport:
    """Manage one foreground master without deciding retry policy."""

    def __init__(self, settings: SshSettings) -> None:
        settings.validate()
        self.settings = settings
        self._master: subprocess.Popen[bytes] | None = None
        self._master_log: Any | None = None

    async def start_master(self) -> None:
        control_path = self.settings.control_path
        control_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        control_path.unlink(missing_ok=True)
        log_path = control_path.with_suffix(control_path.suffix + ".log")
        self._master_log = log_path.open("wb")
        try:
            self._master = await asyncio.to_thread(
                _spawn_master, self._master_argv(), self._master_log
            )
        except OSError as exc:
            raise ConnectionUnavailable(f"could not launch OpenSSH master: {exc}") from exc
        await self._wait_until_ready()

    async def stop_master(self) -> None:
        """Stop either the owned process or an adopted orphan."""

        if await self.control_check():
            await self._control_command("exit")
        process, self._master = self._master, None
        if process is not None:
            for _ in range(20):
                if process.poll() is not None:
                    break
                await asyncio.sleep(0.1)
            if process.poll() is None:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGTERM)
                for _ in range(20):
                    if process.poll() is not None:
                        break
                    await asyncio.sleep(0.1)
            if process.poll() is None:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.wait(timeout=1.0)
        if self._master_log is not None:
            self._master_log.close()
            self._master_log = None
        self.settings.control_path.unlink(missing_ok=True)

    def detach_master(self) -> None:
        """Leave the independent master alive for a replacement gateway."""

        if self._master_log is not None:
            self._master_log.close()
            self._master_log = None

    async def adopt_existing(self) -> bool:
        """Adopt a surviving private master only after a real session probe."""

        if not await self.control_check():
            return False
        probe = await self.session("true", timeout=self.settings.probe_timeout)
        return probe.exit_code == 0

    async def control_check(self) -> bool:
        """Check local master-process liveness without opening a connection."""

        if not self.settings.control_path.exists():
            return False
        result = await self._control_command("check")
        return result.exit_code == 0

    async def probe(self) -> bool:
        """Prove the existing master can open a real command channel."""

        result = await self.session("true", timeout=self.settings.probe_timeout)
        return result.exit_code == 0

    async def session(
        self, command: str, *, stdin: bytes = b"", timeout: float
    ) -> ProcessResult:
        """Run one channel with direct SSH fallback explicitly disabled."""

        argv = [
            self.settings.ssh_binary,
            "-T",
            "-S",
            str(self.settings.control_path),
            "-o",
            "ControlMaster=no",
            "-o",
            "ControlPersist=no",
            "-o",
            "ProxyCommand=/usr/bin/false",
            "-o",
            "ConnectTimeout=5",
            self.settings.host,
            command,
        ]
        try:
            return await run_process(
                argv,
                stdin=stdin,
                timeout=timeout,
                output_limit=self.settings.max_output_bytes,
            )
        except TimeoutError as exc:
            raise CommandTimedOut(f"remote command exceeded its {timeout:g}s deadline") from exc
        except OSError as exc:
            raise ConnectionUnavailable(f"could not launch OpenSSH: {exc}") from exc

    async def _wait_until_ready(self) -> None:
        deadline = time.monotonic() + self.settings.connect_timeout + 5.0
        while time.monotonic() < deadline:
            if self._master is not None and self._master.poll() is not None:
                raise ConnectionUnavailable(self._master_error("SSH master exited"))
            if await self.control_check():
                if await self.probe():
                    return
                raise ConnectionUnavailable(self._master_error("session probe failed"))
            await asyncio.sleep(0.1)
        raise ConnectionUnavailable(self._master_error("SSH master startup timed out"))

    async def _control_command(self, operation: str) -> ProcessResult:
        argv = [
            self.settings.ssh_binary,
            "-S",
            str(self.settings.control_path),
            "-o",
            "ConnectTimeout=5",
            "-O",
            operation,
            self.settings.host,
        ]
        try:
            return await run_process(argv, timeout=5.0, output_limit=4096)
        except (OSError, TimeoutError):
            return ProcessResult(255, b"", b"control operation failed", False, False)

    def _master_argv(self) -> list[str]:
        return [
            self.settings.ssh_binary,
            "-T",
            "-N",
            "-S",
            str(self.settings.control_path),
            "-o",
            "ControlMaster=yes",
            "-o",
            "ControlPersist=no",
            "-o",
            f"ConnectTimeout={self.settings.connect_timeout:g}",
            "-o",
            "ConnectionAttempts=1",
            "-o",
            f"ServerAliveInterval={self.settings.keepalive_interval}",
            "-o",
            f"ServerAliveCountMax={self.settings.keepalive_count}",
            self.settings.host,
        ]

    def _master_error(self, prefix: str) -> str:
        log_path = self.settings.control_path.with_suffix(self.settings.control_path.suffix + ".log")
        detail = ""
        with contextlib.suppress(OSError):
            detail = log_path.read_bytes()[-8000:].decode(errors="replace").strip()
        return f"{prefix}: {detail}" if detail else prefix


__all__ = ["OpenSshTransport", "SshSettings"]

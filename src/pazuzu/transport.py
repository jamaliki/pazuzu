"""Own the raw OpenSSH ControlMaster and command channels."""

from __future__ import annotations

import asyncio
import contextlib
import os
import shlex
import signal
import subprocess
import time
from collections.abc import Mapping
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
class ShellAttachment:
    """Snapshot describing one fail-closed interactive SSH attachment."""

    host: str
    ssh_binary: str
    control_path: Path
    generation: int

    def __post_init__(self) -> None:
        if not self.host or "\x00" in self.host:
            raise ValueError("attachment host must be non-empty and contain no NUL bytes")
        if not self.ssh_binary or "\x00" in self.ssh_binary:
            raise ValueError("attachment SSH executable must be non-empty and contain no NUL bytes")
        if not str(self.control_path) or "\x00" in str(self.control_path):
            raise ValueError("attachment control path must be non-empty and contain no NUL bytes")
        if (
            isinstance(self.generation, bool)
            or not isinstance(self.generation, int)
            or self.generation < 1
        ):
            raise ValueError("attachment generation must be a positive integer")

    def as_dict(self) -> dict[str, str | int]:
        """Return the narrow JSON representation exposed by the gateway."""

        return {
            "host": self.host,
            "ssh_binary": self.ssh_binary,
            "control_path": str(self.control_path),
            "generation": self.generation,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ShellAttachment:
        """Parse a gateway response without accepting extra SSH configuration."""

        expected = {"host", "ssh_binary", "control_path", "generation"}
        if set(value) != expected:
            raise ValueError("invalid shell attachment descriptor fields")
        host = value["host"]
        ssh_binary = value["ssh_binary"]
        control_path = value["control_path"]
        generation = value["generation"]
        if not all(isinstance(item, str) for item in (host, ssh_binary, control_path)):
            raise TypeError("shell attachment paths and host must be strings")
        if not control_path:
            raise ValueError("shell attachment control path must not be empty")
        if isinstance(generation, bool) or not isinstance(generation, int):
            raise TypeError("shell attachment generation must be an integer")
        return cls(
            host=host,
            ssh_binary=ssh_binary,
            control_path=Path(control_path),
            generation=generation,
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
            self._release_master_state()
            raise ConnectionUnavailable(f"could not launch OpenSSH master: {exc}") from exc
        try:
            await self._wait_until_ready()
        except BaseException:
            await self._stop_owned_master()
            self._release_master_state()
            raise

    async def stop_master(self) -> None:
        """Stop either the owned process or an adopted orphan."""

        if await self.control_check():
            await self._control_command("exit")
        await self._stop_owned_master()
        self._release_master_state()

    def _release_master_state(self) -> None:
        if self._master_log is not None:
            self._master_log.close()
            self._master_log = None
        self.settings.control_path.unlink(missing_ok=True)

    async def _stop_owned_master(self) -> None:
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


def _bridge_spec(
    settings: SshSettings,
    *,
    listen_host: str,
    listen_port: int,
    remote_host: str,
    remote_port: int,
) -> str:
    """Validate and serialize one loopback forwarding rule."""

    settings.validate()
    if not listen_host or not remote_host or any(
        "\x00" in value for value in (listen_host, remote_host)
    ):
        raise ValueError("bridge hosts must be non-empty and contain no NUL bytes")
    if not 1 <= listen_port <= 65535 or not 1 <= remote_port <= 65535:
        raise ValueError("bridge ports must be between 1 and 65535")
    return f"{listen_host}:{listen_port}:{remote_host}:{remote_port}"


def bridge_control_argv(
    settings: SshSettings,
    *,
    operation: str,
    listen_host: str,
    listen_port: int,
    remote_host: str,
    remote_port: int,
) -> list[str]:
    """Build a deterministic forward/cancel operation for Pazuzu's master."""

    if operation not in {"forward", "cancel"}:
        raise ValueError("bridge control operation must be forward or cancel")
    forward = _bridge_spec(
        settings,
        listen_host=listen_host,
        listen_port=listen_port,
        remote_host=remote_host,
        remote_port=remote_port,
    )
    return [
        settings.ssh_binary,
        "-S",
        str(settings.control_path),
        "-o",
        "ControlMaster=no",
        "-o",
        "ProxyCommand=/usr/bin/false",
        "-o",
        "ExitOnForwardFailure=yes",
        "-O",
        operation,
        "-L",
        forward,
        settings.host,
    ]


def bridge_session_argv(
    settings: SshSettings,
    *,
    remote_command: list[str],
) -> list[str]:
    """Build the service channel, with direct SSH fallback disabled."""

    settings.validate()
    if not remote_command or any(not item or "\x00" in item for item in remote_command):
        raise ValueError("bridge remote command must contain non-empty arguments")
    lifecycle = (
        'child=; guard=; cleanup() { '
        '[ -z "$guard" ] || kill "$guard" 2>/dev/null; '
        '[ -z "$child" ] || kill -TERM "$child" 2>/dev/null; }; '
        "trap cleanup HUP INT TERM; exec 3<&0; "
        '"$@" </dev/null & child=$!; '
        '(IFS= read -r _ <&3 || kill -TERM "$child" 2>/dev/null) & guard=$!; '
        'wait "$child"; code=$?; '
        'kill "$guard" 2>/dev/null; wait "$guard" 2>/dev/null; exit "$code"'
    )
    wrapped_command = f"exec sh -c {shlex.quote(lifecycle)} pazuzu-bridge "
    wrapped_command += shlex.join(remote_command)
    return [
        settings.ssh_binary,
        "-T",
        "-S",
        str(settings.control_path),
        "-o",
        "ControlMaster=no",
        "-o",
        "ControlPersist=no",
        "-o",
        "ProxyCommand=/usr/bin/false",
        settings.host,
        wrapped_command,
    ]


def _control_bridge(argv: list[str], *, required: bool) -> None:
    try:
        result = subprocess.run(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=10.0,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        if required:
            raise ConnectionUnavailable(f"could not manage bridge forward: {exc}") from exc
        return
    if required and result.returncode != 0:
        detail = result.stdout[-4096:].decode(errors="replace").strip()
        raise ConnectionUnavailable(detail or "could not register bridge forward")


def run_bridge(
    settings: SshSettings,
    *,
    listen_host: str,
    listen_port: int,
    remote_host: str,
    remote_port: int,
    remote_command: list[str],
) -> int:
    """Own one forward and remote service for exactly the same lifetime."""

    control = {
        "listen_host": listen_host,
        "listen_port": listen_port,
        "remote_host": remote_host,
        "remote_port": remote_port,
    }
    cancel_argv = bridge_control_argv(settings, operation="cancel", **control)
    forward_argv = bridge_control_argv(settings, operation="forward", **control)
    session_argv = bridge_session_argv(settings, remote_command=remote_command)

    # A SIGKILL may have prevented a previous instance from cleaning up.
    _control_bridge(cancel_argv, required=False)
    _control_bridge(forward_argv, required=True)
    try:
        # The open stdin pipe is a lease. Its EOF makes the remote wrapper stop
        # the service even if a multiplexed SSH channel otherwise survives.
        child = subprocess.Popen(session_argv, stdin=subprocess.PIPE)

        def relay_signal(signum: int, _frame: object) -> None:
            if child.poll() is None and child.stdin is not None:
                child.stdin.close()

        previous = {
            signum: signal.signal(signum, relay_signal)
            for signum in (signal.SIGINT, signal.SIGTERM)
        }
        try:
            return child.wait()
        finally:
            if child.stdin is not None and not child.stdin.closed:
                child.stdin.close()
            for signum, handler in previous.items():
                signal.signal(signum, handler)
    finally:
        _control_bridge(cancel_argv, required=False)


__all__ = [
    "OpenSshTransport",
    "ShellAttachment",
    "SshSettings",
    "bridge_control_argv",
    "bridge_session_argv",
    "run_bridge",
]

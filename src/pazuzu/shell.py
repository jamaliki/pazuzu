"""Interactive shell attachment and reconnect policy."""

from __future__ import annotations

import asyncio
import contextlib
import re
import shlex
import signal
import sys
from collections.abc import Awaitable, Callable, Mapping
from typing import TextIO, TypeVar

from .errors import ConnectionUnavailable, PazuzuError, ProtocolError
from .transport import ShellAttachment

SESSION_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
TMUX_REQUIRED_MESSAGE = "pazuzu shell requires tmux on the remote host"
AUTHENTICATION_MESSAGE = """pazuzu: SSH reauthorization is required.
Complete the configured provider login in another terminal, then run
`pazuzu reconnect`. Waiting to reattach; press Ctrl-C to stop.
"""

GetAttachment = Callable[[], Awaitable[ShellAttachment]]
GatewayCall = Callable[[], Awaitable[Mapping[str, object]]]
SpawnSsh = Callable[[list[str], asyncio.Event], Awaitable[int]]
Sleep = Callable[[float], Awaitable[None]]
T = TypeVar("T")


class _LocalCancellation(Exception):
    pass


class _ConnectionReporter:
    def __init__(self, stream: TextIO) -> None:
        self.stream = stream
        self.last_state: str | None = None

    def connection_lost(self) -> None:
        self._write("pazuzu: connection lost; remote tmux session is still running\n")
        self.last_state = "lost"

    def state_changed(self, state: str) -> None:
        if state == self.last_state:
            return
        self.last_state = state
        if state == "connected":
            self._write("pazuzu: connection restored; reattaching\n")
        elif state == "authentication_required":
            self._write(AUTHENTICATION_MESSAGE)
        elif state == "reconnecting":
            self._write("pazuzu: reconnecting SSH; waiting to reattach\n")
        elif state == "offline":
            self._write("pazuzu: SSH connection is unavailable; waiting to reattach\n")
        else:
            self._write(f"pazuzu: SSH connection state is {state}; waiting to reattach\n")

    def _write(self, message: str) -> None:
        self.stream.write(message)
        self.stream.flush()


def validate_session_name(session: str) -> str:
    """Accept one deliberately narrow tmux session name."""

    if not isinstance(session, str) or SESSION_PATTERN.fullmatch(session) is None:
        raise ValueError(
            "session name must be 1-64 ASCII letters, digits, '.', '_', or '-', "
            "and start with a letter or digit"
        )
    return session


def remote_tmux_command(session: str) -> str:
    """Build the validated remote shell wrapper."""

    session = validate_session_name(session)
    tmux_command = shlex.join(["tmux", "new-session", "-A", "-s", session])
    return (
        "command -v tmux >/dev/null 2>&1 || {\n"
        f"    printf '%s\\n' {shlex.quote(TMUX_REQUIRED_MESSAGE)} >&2\n"
        "    exit 127\n"
        "}\n"
        f"exec {tmux_command}"
    )


def shell_argv(attachment: ShellAttachment, session: str) -> list[str]:
    """Build an interactive SSH command that cannot open a direct connection."""

    return [
        attachment.ssh_binary,
        "-tt",
        "-S",
        str(attachment.control_path),
        "-o",
        "ControlMaster=no",
        "-o",
        "ControlPersist=no",
        "-o",
        "ProxyCommand=/usr/bin/false",
        "-o",
        "ConnectTimeout=5",
        "--",
        attachment.host,
        remote_tmux_command(session),
    ]


def require_interactive_terminal(
    stdin: TextIO | None = None, stdout: TextIO | None = None
) -> None:
    """Reject shell use unless both terminal directions are interactive."""

    stdin = sys.stdin if stdin is None else stdin
    stdout = sys.stdout if stdout is None else stdout
    if not stdin.isatty() or not stdout.isatty():
        raise ValueError("pazuzu shell requires an interactive terminal on stdin and stdout")


def _install_signal_handler(
    signum: signal.Signals, callback: Callable[[], None]
) -> signal.Handlers:
    loop = asyncio.get_running_loop()
    previous = signal.getsignal(signum)
    loop.add_signal_handler(signum, callback)
    return previous


def _restore_signal_handler(signum: signal.Signals, previous: signal.Handlers) -> None:
    asyncio.get_running_loop().remove_signal_handler(signum)
    signal.signal(signum, previous)


async def _terminate_attached_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        process.terminate()
    try:
        await asyncio.wait_for(process.wait(), 2.0)
    except TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            process.kill()
        await process.wait()


def _normalized_exit_code(returncode: int) -> int:
    return 128 - returncode if returncode < 0 else returncode


async def run_attached_ssh(argv: list[str], cancellation: asyncio.Event) -> int:
    """Run one SSH client on the real terminal while keeping local cancellation distinct."""

    if not argv or any(not item or "\x00" in item for item in argv):
        raise ValueError("interactive SSH arguments must be non-empty and contain no NUL bytes")
    if cancellation.is_set():
        return 130

    previous_interrupt = _install_signal_handler(signal.SIGINT, lambda: None)
    process: asyncio.subprocess.Process | None = None
    wait_task: asyncio.Task[int] | None = None
    cancel_task: asyncio.Task[bool] | None = None
    try:
        try:
            process = await asyncio.create_subprocess_exec(*argv)
        except OSError as exc:
            raise ConnectionUnavailable(f"could not launch OpenSSH: {exc}") from exc
        wait_task = asyncio.create_task(process.wait())
        cancel_task = asyncio.create_task(cancellation.wait())
        done, _ = await asyncio.wait(
            (wait_task, cancel_task), return_when=asyncio.FIRST_COMPLETED
        )
        if cancel_task in done:
            await _terminate_attached_process(process)
            await wait_task
            return 130
        cancel_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await cancel_task
        return _normalized_exit_code(wait_task.result())
    except asyncio.CancelledError:
        if process is not None:
            await _terminate_attached_process(process)
        raise
    finally:
        for task in (wait_task, cancel_task):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (wait_task, cancel_task) if task is not None),
            return_exceptions=True,
        )
        _restore_signal_handler(signal.SIGINT, previous_interrupt)


async def _call_or_stop(
    operation: Callable[[], Awaitable[T]], cancellation: asyncio.Event
) -> T:
    if cancellation.is_set():
        raise _LocalCancellation
    operation_task = asyncio.create_task(operation())
    cancel_task = asyncio.create_task(cancellation.wait())
    try:
        done, _ = await asyncio.wait(
            (operation_task, cancel_task), return_when=asyncio.FIRST_COMPLETED
        )
        if cancel_task in done:
            raise _LocalCancellation
        return operation_task.result()
    finally:
        for task in (operation_task, cancel_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(operation_task, cancel_task, return_exceptions=True)


def _connection_state(health: Mapping[str, object]) -> str:
    state = health.get("connection")
    if not isinstance(state, str) or not state:
        raise ProtocolError("gateway status has no valid connection state")
    return state


async def _wait_until_connected(
    *,
    status: GatewayCall,
    sleep: Sleep,
    poll_interval: float,
    cancellation: asyncio.Event,
    reporter: _ConnectionReporter,
    initial_health: Mapping[str, object] | None = None,
) -> None:
    health = initial_health
    while True:
        if health is None:
            health = await _call_or_stop(status, cancellation)
        state = _connection_state(health)
        reporter.state_changed(state)
        if state == "connected":
            return
        await _call_or_stop(lambda: sleep(poll_interval), cancellation)
        health = None


async def _ready_attachment(
    *,
    get_attachment: GetAttachment,
    status: GatewayCall,
    sleep: Sleep,
    poll_interval: float,
    cancellation: asyncio.Event,
    reporter: _ConnectionReporter,
    initial_health: Mapping[str, object] | None = None,
) -> ShellAttachment:
    health = initial_health or await _call_or_stop(status, cancellation)
    while True:
        state = _connection_state(health)
        if state != "connected":
            await _wait_until_connected(
                status=status,
                sleep=sleep,
                poll_interval=poll_interval,
                cancellation=cancellation,
                reporter=reporter,
                initial_health=health,
            )
        elif reporter.last_state is not None:
            reporter.state_changed(state)
        try:
            return await _call_or_stop(get_attachment, cancellation)
        except PazuzuError:
            health = await _call_or_stop(status, cancellation)
            if _connection_state(health) == "connected":
                raise


async def run_shell(
    session: str,
    *,
    get_attachment: GetAttachment,
    reconnect: GatewayCall,
    status: GatewayCall,
    spawn_ssh: SpawnSsh = run_attached_ssh,
    sleep: Sleep = asyncio.sleep,
    poll_interval: float = 1.0,
    stderr: TextIO = sys.stderr,
    cancellation: asyncio.Event | None = None,
) -> int:
    """Attach and reattach one named remote tmux session until a terminal exit."""

    session = validate_session_name(session)
    if poll_interval <= 0:
        raise ValueError("shell poll interval must be positive")
    owns_cancellation = cancellation is None
    cancellation = asyncio.Event() if cancellation is None else cancellation
    previous_termination: signal.Handlers | None = None
    if owns_cancellation:
        previous_termination = _install_signal_handler(signal.SIGTERM, cancellation.set)
    reporter = _ConnectionReporter(stderr)
    initial_health: Mapping[str, object] | None = None
    try:
        while True:
            attachment = await _ready_attachment(
                get_attachment=get_attachment,
                status=status,
                sleep=sleep,
                poll_interval=poll_interval,
                cancellation=cancellation,
                reporter=reporter,
                initial_health=initial_health,
            )
            initial_health = None
            exit_code = await spawn_ssh(shell_argv(attachment, session), cancellation)
            if cancellation.is_set():
                return 130
            if exit_code != 255:
                return exit_code

            reporter.connection_lost()
            try:
                initial_health = await _call_or_stop(reconnect, cancellation)
            except PazuzuError:
                initial_health = None
    except _LocalCancellation:
        return 130
    finally:
        if previous_termination is not None:
            _restore_signal_handler(signal.SIGTERM, previous_termination)


__all__ = [
    "remote_tmux_command",
    "require_interactive_terminal",
    "run_attached_ssh",
    "run_shell",
    "shell_argv",
    "validate_session_name",
]

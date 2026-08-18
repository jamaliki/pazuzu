"""Native file transfers through Pazuzu's existing OpenSSH master."""

from __future__ import annotations

import asyncio
import contextlib
import re
import shlex
from collections.abc import Sequence

from .transport import SshAttachment

_REMOTE_SPEC = re.compile(r"^[^/][^:]*:")
_COMMON_SSH_OPTIONS = (
    "ControlMaster=no",
    "ControlPersist=no",
    "ProxyCommand=/usr/bin/false",
    "ConnectTimeout=5",
    "BatchMode=yes",
)


def _arguments(values: Sequence[str]) -> list[str]:
    arguments = list(values)
    if arguments[:1] == ["--"]:
        arguments.pop(0)
    if not arguments:
        raise ValueError("provide transfer arguments after '--'")
    return arguments


def _reject_transport_overrides(tool: str, arguments: Sequence[str]) -> None:
    for argument in arguments:
        if tool == "cp" and (
            argument in {"-D", "-F", "-J", "-S", "-o"}
            or argument.startswith(("-D", "-F", "-J", "-S", "-o"))
        ):
            raise ValueError("pazuzu cp does not allow SSH transport overrides")
        if tool == "rsync" and (
            argument == "-e"
            or argument.startswith("--rsh")
            or (argument.startswith("-") and not argument.startswith("--") and "e" in argument[1:])
        ):
            raise ValueError("pazuzu rsync does not allow a remote-shell override")


def _remote_arguments(host: str, values: Sequence[str], *, tool: str) -> list[str]:
    arguments = _arguments(values)
    _reject_transport_overrides(tool, arguments)
    rewritten: list[str] = []
    remote_count = 0
    for argument in arguments:
        if argument.startswith(":") and not argument.startswith("::"):
            if len(argument) == 1:
                raise ValueError("remote paths must not be empty")
            rewritten.append(f"{host}:{argument[1:]}")
            remote_count += 1
        elif not argument.startswith("-") and (
            "://" in argument or "::" in argument or _REMOTE_SPEC.match(argument)
        ):
            raise ValueError(
                "remote paths must use the Pazuzu ':path' form; other hosts are not allowed"
            )
        else:
            rewritten.append(argument)
    if remote_count != 1:
        raise ValueError("provide exactly one remote path using the Pazuzu ':path' form")
    return rewritten


def _ssh_options(attachment: SshAttachment) -> list[str]:
    options: list[str] = []
    for value in _COMMON_SSH_OPTIONS:
        options.extend(("-o", value))
    options.extend(("-o", f"ControlPath={attachment.control_path}"))
    return options


def copy_argv(
    attachment: SshAttachment,
    arguments: Sequence[str],
    *,
    executable: str = "/usr/bin/scp",
) -> list[str]:
    """Build an scp command that fails rather than opening another SSH connection."""

    return [
        executable,
        "-S",
        attachment.ssh_binary,
        *_ssh_options(attachment),
        *_remote_arguments(attachment.host, arguments, tool="cp"),
    ]


def rsync_argv(
    attachment: SshAttachment,
    arguments: Sequence[str],
    *,
    executable: str = "/usr/bin/rsync",
) -> list[str]:
    """Build an rsync command whose remote shell is Pazuzu's current master."""

    remote_shell = shlex.join(
        [attachment.ssh_binary, *_ssh_options(attachment)]
    )
    return [
        executable,
        "-e",
        remote_shell,
        *_remote_arguments(attachment.host, arguments, tool="rsync"),
    ]


async def _terminate(process: asyncio.subprocess.Process) -> None:
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


async def run_transfer(argv: Sequence[str]) -> int:
    """Run a native transfer on the caller's terminal without buffering its bytes."""

    process = await asyncio.create_subprocess_exec(*argv)
    try:
        returncode = await process.wait()
    except asyncio.CancelledError:
        await _terminate(process)
        raise
    return 128 - returncode if returncode < 0 else returncode


__all__ = ["copy_argv", "rsync_argv", "run_transfer"]

"""Run and cancel bounded local subprocesses."""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
from dataclasses import dataclass


@dataclass(frozen=True)
class ProcessResult:
    exit_code: int
    stdout: bytes
    stderr: bytes
    stdout_truncated: bool
    stderr_truncated: bool


async def _read_bounded(
    stream: asyncio.StreamReader | None, limit: int
) -> tuple[bytes, bool]:
    if stream is None:
        return b"", False
    output = bytearray()
    truncated = False
    while chunk := await stream.read(64 * 1024):
        remaining = limit - len(output)
        if remaining > 0:
            output.extend(chunk[:remaining])
        truncated = truncated or len(chunk) > remaining
    return bytes(output), truncated


async def _feed_stdin(writer: asyncio.StreamWriter | None, content: bytes) -> None:
    if writer is None:
        return
    try:
        writer.write(content)
        await writer.drain()
    except (BrokenPipeError, ConnectionResetError):
        pass
    finally:
        writer.close()


async def stop_process(process: asyncio.subprocess.Process) -> None:
    """Terminate a process group, escalating after two seconds."""

    if process.returncode is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    try:
        await asyncio.wait_for(process.wait(), 2.0)
    except TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        await process.wait()


async def run_process(
    argv: list[str],
    *,
    stdin: bytes = b"",
    timeout: float,
    output_limit: int,
) -> ProcessResult:
    """Run one process with bounded output and cancellation-safe cleanup."""

    process = await asyncio.create_subprocess_exec(
        *argv,
        stdin=asyncio.subprocess.PIPE if stdin else asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    readers = (
        asyncio.create_task(_read_bounded(process.stdout, output_limit)),
        asyncio.create_task(_read_bounded(process.stderr, output_limit)),
    )
    feeder = asyncio.create_task(_feed_stdin(process.stdin, stdin))
    try:
        async with asyncio.timeout(timeout):
            await process.wait()
            stdout, stderr = await asyncio.gather(*readers)
            await feeder
    except (TimeoutError, asyncio.CancelledError):
        await stop_process(process)
        await asyncio.gather(*readers, feeder, return_exceptions=True)
        raise
    return ProcessResult(process.returncode, stdout[0], stderr[0], stdout[1], stderr[1])


__all__ = ["ProcessResult", "run_process", "stop_process"]

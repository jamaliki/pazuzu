"""Small public client for a running Pazuzu gateway."""

from __future__ import annotations

import shlex
from collections.abc import Sequence
from pathlib import Path, PurePosixPath
from typing import Any

from .errors import PazuzuError
from .gateway import call_gateway, default_socket_path, execute_via_gateway
from .slurm import (
    SlurmHandle,
    SlurmJob,
    SlurmStatus,
    parse_sacct,
    parse_squeue,
    parse_submission,
    render_slurm_script,
    validate_job_id,
)
from .supervisor import CommandResult


class PazuzuClient:
    """Run commands through Pazuzu without owning an SSH connection."""

    def __init__(self, socket_path: str | Path | None = None) -> None:
        self.socket_path = Path(socket_path or default_socket_path()).expanduser().resolve()

    async def health(self, *, probe: bool = True, timeout: float = 90.0) -> dict[str, Any]:
        """Return gateway, authentication, and real-session health."""

        return await call_gateway(
            self.socket_path, "status", {"probe": probe}, timeout=timeout
        )

    async def reconnect(self) -> dict[str, Any]:
        """Reconnect immediately, typically after interactive reauthorization."""

        return await call_gateway(self.socket_path, "reconnect", timeout=140.0)

    async def run(
        self,
        command: str,
        *,
        stdin: str | bytes = b"",
        timeout: float = 120.0,
    ) -> CommandResult:
        """Run a command once; never replay after an uncertain disconnect."""

        return await self._execute(command, stdin=stdin, timeout=timeout, retry_safe=False)

    async def read(
        self,
        command: str,
        *,
        stdin: str | bytes = b"",
        timeout: float = 120.0,
    ) -> CommandResult:
        """Run an idempotent read, allowing one replay after proven repair."""

        return await self._execute(command, stdin=stdin, timeout=timeout, retry_safe=True)

    async def run_script(
        self,
        interpreter: str,
        source: str | bytes,
        *,
        arguments: Sequence[str] = (),
        timeout: float = 120.0,
    ) -> CommandResult:
        """Stream source to an interpreter without replaying it."""

        if not interpreter or "\x00" in interpreter:
            raise ValueError("interpreter must be a non-empty executable without NUL bytes")
        command = shlex.join([interpreter, "-", *arguments])
        return await self.run(command, stdin=source, timeout=timeout)

    async def slurm_queue(self) -> CommandResult:
        """Return the current remote user's compact Slurm queue."""

        return await self.read(
            'squeue -u "$USER" -o "%.18i %.28j %.10T %.12M %.10l %.20R"',
            timeout=30.0,
        )

    async def inspect_slurm_job(self, job_id: str) -> CommandResult:
        """Return `scontrol show job -o` output for one numeric job ID."""

        job_id = validate_job_id(job_id)
        return await self.read(
            f"scontrol show job -o {shlex.quote(job_id)}", timeout=30.0
        )

    async def submit_slurm(self, job: SlurmJob) -> SlurmHandle:
        """Submit one rendered job through stdin without automatic replay."""

        command = f"mkdir -p -- {shlex.quote(job.log_dir)} && sbatch --parsable"
        result = await self.run(command, stdin=render_slurm_script(job), timeout=30.0)
        self._require_success(result, "sbatch")
        try:
            return parse_submission(result.stdout, job)
        except ValueError as exc:
            raise PazuzuError(str(exc)) from exc

    async def slurm_status(self, job_id: str) -> SlurmStatus:
        """Return compact live or accounting status for one Slurm job."""

        job_id = validate_job_id(job_id)
        queued = await self.read(
            f"squeue -h -j {job_id} -o '%i|%T|%M|%l|%R'", timeout=30.0
        )
        if queued.exit_code == 0:
            status = parse_squeue(job_id, queued.stdout)
            if status is not None:
                return status

        accounted = await self.read(
            f"sacct -n -P -X -j {job_id} --format=JobIDRaw,State,Elapsed,ExitCode",
            timeout=30.0,
        )
        self._require_success(accounted, "sacct")
        status = parse_sacct(job_id, accounted.stdout)
        if status is not None:
            return status
        self._require_success(queued, "squeue")
        return SlurmStatus(job_id=job_id, state="UNKNOWN", source="none")

    async def cancel_slurm(self, job_id: str) -> CommandResult:
        """Cancel one job without replaying an uncertain `scancel`."""

        job_id = validate_job_id(job_id)
        result = await self.run(f"scancel -- {job_id}", timeout=30.0)
        self._require_success(result, "scancel")
        return result

    async def tail(self, remote_path: str, *, lines: int = 100) -> CommandResult:
        """Return at most 1,000 final lines from one absolute remote path."""

        if not PurePosixPath(remote_path).is_absolute() or "\x00" in remote_path:
            raise ValueError("remote_path must be an absolute POSIX path")
        if isinstance(lines, bool) or not 1 <= lines <= 1000:
            raise ValueError("lines must be between 1 and 1000")
        return await self.read(
            f"tail -n {lines} -- {shlex.quote(remote_path)}", timeout=30.0
        )

    async def _execute(
        self,
        command: str,
        *,
        stdin: str | bytes,
        timeout: float,
        retry_safe: bool,
    ) -> CommandResult:
        content = stdin.encode() if isinstance(stdin, str) else stdin
        result = await execute_via_gateway(
            self.socket_path,
            command,
            stdin=content,
            timeout=timeout,
            retry_safe=retry_safe,
        )
        return CommandResult(**result)

    @staticmethod
    def _require_success(result: CommandResult, operation: str) -> None:
        if result.exit_code != 0:
            detail = result.stderr.strip() or result.stdout.strip() or "no remote error output"
            raise PazuzuError(f"{operation} failed with exit {result.exit_code}: {detail}")


__all__ = ["PazuzuClient"]

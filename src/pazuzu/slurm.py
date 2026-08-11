"""Pure types, rendering, and parsing for generic Slurm jobs."""

from __future__ import annotations

import posixpath
import re
import shlex
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import Any

_JOB_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_ENVIRONMENT_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_TERMINAL_STATES = {
    "BOOT_FAIL",
    "CANCELLED",
    "COMPLETED",
    "DEADLINE",
    "FAILED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "REVOKED",
    "SPECIAL_EXIT",
    "TIMEOUT",
}


def _positive(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")


def _nonnegative(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")


def _directive(name: str, value: str | None) -> None:
    if value is not None and (
        not isinstance(value, str) or not value or any(character.isspace() for character in value)
    ):
        raise ValueError(f"{name} must be a non-empty Slurm token without whitespace")


def _absolute(name: str, value: str) -> None:
    if not isinstance(value, str) or "\x00" in value or not PurePosixPath(value).is_absolute():
        raise ValueError(f"{name} must be an absolute POSIX path")


def _slurm_path(name: str, value: str) -> None:
    _absolute(name, value)
    if any(character.isspace() for character in value):
        raise ValueError(f"{name} must not contain whitespace in an sbatch directive")


def validate_job_id(job_id: str) -> str:
    """Return one safe numeric Slurm job ID."""

    if not job_id.isascii() or not job_id.isdecimal():
        raise ValueError("job_id must contain ASCII decimal digits")
    return job_id


@dataclass(frozen=True)
class SlurmResources:
    """Portable Slurm resources with explicit host memory and walltime."""

    memory_gb_per_node: int
    time_limit: str
    cpus_per_task: int = 1
    gpus_per_node: int = 0
    nodes: int = 1
    tasks_per_node: int = 1
    partition: str | None = None
    account: str | None = None
    qos: str | None = None
    constraint: str | None = None
    array: str | None = None
    dependency: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.time_limit, str):
            raise TypeError("time_limit must be a string")
        _positive("memory_gb_per_node", self.memory_gb_per_node)
        _positive("cpus_per_task", self.cpus_per_task)
        _nonnegative("gpus_per_node", self.gpus_per_node)
        _positive("nodes", self.nodes)
        _positive("tasks_per_node", self.tasks_per_node)
        for name in (
            "time_limit",
            "partition",
            "account",
            "qos",
            "constraint",
            "array",
            "dependency",
        ):
            _directive(name, getattr(self, name))


@dataclass(frozen=True)
class SlurmJob:
    """One shell-free command and its generic Slurm execution context."""

    name: str
    argv: Sequence[str]
    cwd: str
    log_dir: str
    resources: SlurmResources
    environment: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not _JOB_NAME.fullmatch(self.name):
            raise ValueError("name must contain only ASCII letters, digits, '.', '_' or '-'")
        if not isinstance(self.resources, SlurmResources):
            raise TypeError("resources must be SlurmResources")
        if isinstance(self.argv, (str, bytes)):
            raise TypeError("argv must be a sequence of strings")
        argv = tuple(self.argv)
        if not argv or any(
            not isinstance(item, str) or not item or "\x00" in item for item in argv
        ):
            raise ValueError("argv must contain non-empty strings without NUL bytes")
        if not isinstance(self.environment, Mapping):
            raise TypeError("environment must be a mapping")
        environment = dict(self.environment)
        _absolute("cwd", self.cwd)
        _slurm_path("log_dir", self.log_dir)
        for name, value in environment.items():
            if (
                not isinstance(name, str)
                or not isinstance(value, str)
                or not _ENVIRONMENT_NAME.fullmatch(name)
                or "\x00" in value
            ):
                raise ValueError(
                    "environment must contain valid names and values without NUL bytes"
                )
        object.__setattr__(self, "argv", argv)
        object.__setattr__(self, "environment", MappingProxyType(environment))

    @property
    def stdout_pattern(self) -> str:
        return posixpath.join(self.log_dir, f"{self.name}-%j.out")

    @property
    def stderr_pattern(self) -> str:
        return posixpath.join(self.log_dir, f"{self.name}-%j.err")


@dataclass(frozen=True)
class SlurmHandle:
    """Stable identity and log locations returned after submission."""

    job_id: str
    name: str
    stdout_path: str
    stderr_path: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SlurmStatus:
    """Compact structured status from live queue or accounting."""

    job_id: str
    state: str
    source: str
    elapsed: str | None = None
    time_limit: str | None = None
    reason: str | None = None
    exit_code: str | None = None

    @property
    def terminal(self) -> bool:
        return self.state in _TERMINAL_STATES

    def as_dict(self) -> dict[str, Any]:
        return {**asdict(self), "terminal": self.terminal}


def render_slurm_script(job: SlurmJob) -> str:
    """Render one deterministic sbatch script without touching the filesystem."""

    resources = job.resources
    lines = [
        "#!/usr/bin/env bash",
        f"#SBATCH --job-name={job.name}",
    ]
    for option, value in (
        ("partition", resources.partition),
        ("account", resources.account),
        ("qos", resources.qos),
        ("constraint", resources.constraint),
    ):
        if value is not None:
            lines.append(f"#SBATCH --{option}={value}")
    lines.extend(
        [
            f"#SBATCH --nodes={resources.nodes}",
            f"#SBATCH --ntasks-per-node={resources.tasks_per_node}",
            f"#SBATCH --cpus-per-task={resources.cpus_per_task}",
            f"#SBATCH --mem={resources.memory_gb_per_node}G",
            f"#SBATCH --time={resources.time_limit}",
        ]
    )
    if resources.gpus_per_node:
        lines.append(f"#SBATCH --gres=gpu:{resources.gpus_per_node}")
    if resources.array is not None:
        lines.append(f"#SBATCH --array={resources.array}")
    if resources.dependency is not None:
        lines.append(f"#SBATCH --dependency={resources.dependency}")
    lines.extend(
        [
            f"#SBATCH --output={job.stdout_pattern}",
            f"#SBATCH --error={job.stderr_pattern}",
            "",
            "set -euo pipefail",
            f"cd {shlex.quote(job.cwd)}",
        ]
    )
    for name, value in sorted(job.environment.items()):
        lines.append(f"export {name}={shlex.quote(value)}")
    lines.extend(("", f"exec {shlex.join(job.argv)}", ""))
    return "\n".join(lines)


def parse_submission(stdout: str, job: SlurmJob) -> SlurmHandle:
    """Parse `sbatch --parsable` output into a stable handle."""

    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    if not lines:
        raise ValueError("sbatch did not return a job ID")
    job_id = validate_job_id(lines[-1].split(";", 1)[0])
    return SlurmHandle(
        job_id=job_id,
        name=job.name,
        stdout_path=job.stdout_pattern.replace("%j", job_id),
        stderr_path=job.stderr_pattern.replace("%j", job_id),
    )


def parse_squeue(job_id: str, stdout: str) -> SlurmStatus | None:
    """Parse Pazuzu's compact `squeue` format."""

    for line in stdout.splitlines():
        values = line.strip().split("|", 4)
        if len(values) == 5 and values[0].split("_", 1)[0] == job_id:
            return SlurmStatus(
                job_id=job_id,
                state=values[1],
                source="squeue",
                elapsed=values[2],
                time_limit=values[3],
                reason=values[4] or None,
            )
    return None


def parse_sacct(job_id: str, stdout: str) -> SlurmStatus | None:
    """Parse Pazuzu's compact `sacct` format, ignoring job steps."""

    for line in stdout.splitlines():
        values = line.strip().split("|", 3)
        if len(values) == 4 and values[0] == job_id:
            state = values[1].split(maxsplit=1)[0].rstrip("+")
            return SlurmStatus(
                job_id=job_id,
                state=state,
                source="sacct",
                elapsed=values[2] or None,
                exit_code=values[3] or None,
            )
    return None


__all__ = [
    "SlurmHandle",
    "SlurmJob",
    "SlurmResources",
    "SlurmStatus",
    "parse_sacct",
    "parse_squeue",
    "parse_submission",
    "render_slurm_script",
    "validate_job_id",
]

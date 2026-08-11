"""Pazuzu: a small self-healing OpenSSH gateway."""

from .client import PazuzuClient
from .slurm import SlurmHandle, SlurmJob, SlurmResources, SlurmStatus
from .supervisor import CommandResult

__version__ = "0.1.0"

__all__ = [
    "CommandResult",
    "PazuzuClient",
    "SlurmHandle",
    "SlurmJob",
    "SlurmResources",
    "SlurmStatus",
]

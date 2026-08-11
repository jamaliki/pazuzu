"""Stable public imports for Pazuzu's OpenSSH supervisor."""

from .supervisor import CommandResult, OpenSshSupervisor, classify_connection_failure
from .transport import ShellAttachment, SshSettings

__all__ = [
    "CommandResult",
    "OpenSshSupervisor",
    "ShellAttachment",
    "SshSettings",
    "classify_connection_failure",
]

"""Stable public imports for Pazuzu's OpenSSH supervisor."""

from .supervisor import CommandResult, OpenSshSupervisor, classify_connection_failure
from .transport import ShellAttachment, SshAttachment, SshSettings

__all__ = [
    "CommandResult",
    "OpenSshSupervisor",
    "ShellAttachment",
    "SshAttachment",
    "SshSettings",
    "classify_connection_failure",
]

"""Public Pazuzu error types."""

from __future__ import annotations


class PazuzuError(RuntimeError):
    """Base class for expected gateway failures."""


class ConnectionUnavailable(PazuzuError):
    """The SSH master could not become usable."""


class CommandTimedOut(PazuzuError):
    """A bounded remote command exceeded its deadline."""


class UncertainExecution(PazuzuError):
    """Transport failed after a command may have begun remotely."""


class ProtocolError(PazuzuError):
    """A local gateway request or response was malformed."""

"""Cross-process leases for SSH channels sharing one ControlMaster."""

from __future__ import annotations

import asyncio
import contextlib
import fcntl
import os
import time
from pathlib import Path
from typing import Self


class SessionLease:
    """One held slot in a :class:`SessionLeasePool`."""

    def __init__(self, handle: object) -> None:
        self._handle = handle

    def release(self) -> None:
        handle = self._handle
        if handle is None:
            return
        self._handle = None
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)  # type: ignore[union-attr]
        handle.close()  # type: ignore[union-attr]

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.release()


class SessionLeasePool:
    """A small, crash-safe, advisory-lock pool shared by gateway clients.

    A held `flock` is released by the kernel when its process exits, so a
    crashed bridge cannot permanently consume capacity.  The pool directory
    is intentionally derived from the private control socket.
    """

    def __init__(
        self, directory: Path, size: int, *, start_slot: int = 0, prefix: str = "slot"
    ) -> None:
        if size < 1:
            raise ValueError("session lease pool size must be positive")
        if start_slot < 0:
            raise ValueError("session lease pool start slot must not be negative")
        self.directory = directory
        self.size = size
        self.start_slot = start_slot
        self.prefix = prefix

    def acquire(self, *, timeout: float | None = None) -> SessionLease:
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.directory.chmod(0o700)
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            lease = self._try_acquire()
            if lease is not None:
                return lease
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("no Pazuzu SSH session lease became available")
            time.sleep(0.05)

    async def acquire_async(self, *, timeout: float | None = None) -> SessionLease:
        """Acquire without an un-cancellable worker thread."""

        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.directory.chmod(0o700)
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            lease = self._try_acquire()
            if lease is not None:
                return lease
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("no Pazuzu SSH session lease became available")
            await asyncio.sleep(0.05)

    def _try_acquire(self) -> SessionLease | None:
        for index in range(self.start_slot, self.start_slot + self.size):
            target = self.directory / f"{self.prefix}-{index}"
            handle = target.open("a+")
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                handle.close()
                continue
            os.chmod(target, 0o600)
            return SessionLease(handle)
        return None


def session_lease_directory(control_path: Path) -> Path:
    """Return the private lease directory associated with a control socket."""

    return control_path.with_suffix(control_path.suffix + ".sessions")


_PUBLISHED = "max-sessions"


class SessionBudget:
    """Divide one master's session channels between its clients.

    ``max_sessions`` counts every channel Pazuzu itself opens on the master:
    long-lived bridges, gateway commands and transfers, and health probes.
    Interactive shells attach outside this budget and use the server's
    remaining headroom.

    One session belongs to health probes alone, so a probe can never be
    refused because ordinary work filled the budget and capacity pressure is
    not mistaken for a broken master.  Its lock file does not depend on the
    budget, so no client can take it by computing a different layout.  The
    rest are operation slots, and bridges may not take the last of them, so
    long-lived bridges can never starve gateway operations.
    """

    def __init__(self, control_path: Path, max_sessions: int) -> None:
        if max_sessions < 2:
            raise ValueError("max_sessions must leave one probe and one operation slot")
        self.directory = session_lease_directory(control_path)
        self.max_sessions = max_sessions

    @classmethod
    def published(cls, control_path: Path, default: int) -> SessionBudget:
        """The budget the gateway last published, or ``default`` before any has."""

        try:
            text = (session_lease_directory(control_path) / _PUBLISHED).read_text("ascii")
            value = int(text.strip())
        except (OSError, ValueError):
            value = default
        return cls(control_path, value if 2 <= value <= 64 else default)

    def publish(self) -> None:
        """Record this gateway's budget so bridges divide it the same way."""

        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        target = self.directory / _PUBLISHED
        temporary = self.directory / f".{_PUBLISHED}.{os.getpid()}.tmp"
        try:
            temporary.write_text(f"{self.max_sessions}\n", "ascii")
            os.chmod(temporary, 0o600)
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)

    def operations(self) -> SessionLeasePool:
        """Slots shared by gateway operations and bridges."""

        return SessionLeasePool(self.directory, self.max_sessions - 1)

    def bridges(self) -> SessionLeasePool:
        """The operation slots a bridge may hold, always leaving one for the gateway."""

        if self.max_sessions < 3:
            raise ValueError("bridges need max_sessions of at least 3")
        return SessionLeasePool(self.directory, self.max_sessions - 2)

    def probes(self) -> SessionLeasePool:
        """The one slot reserved for health probes."""

        return SessionLeasePool(self.directory, 1, prefix="probe")


@contextlib.contextmanager
def held_session_lease(pool: SessionLeasePool):
    lease = pool.acquire()
    try:
        yield lease
    finally:
        lease.release()


__all__ = [
    "SessionBudget",
    "SessionLease",
    "SessionLeasePool",
    "held_session_lease",
    "session_lease_directory",
]

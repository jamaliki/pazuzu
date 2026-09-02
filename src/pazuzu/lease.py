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

    def __init__(self, directory: Path, size: int, *, start_slot: int = 0) -> None:
        if size < 1:
            raise ValueError("session lease pool size must be positive")
        if start_slot < 0:
            raise ValueError("session lease pool start slot must not be negative")
        self.directory = directory
        self.size = size
        self.start_slot = start_slot

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
            target = self.directory / f"slot-{index}"
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


@contextlib.contextmanager
def held_session_lease(pool: SessionLeasePool):
    lease = pool.acquire()
    try:
        yield lease
    finally:
        lease.release()


__all__ = ["SessionLease", "SessionLeasePool", "held_session_lease", "session_lease_directory"]

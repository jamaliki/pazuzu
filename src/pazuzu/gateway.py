"""Small newline-delimited JSON gateway over a private Unix socket."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import os
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .errors import PazuzuError, ProtocolError
from .ssh import OpenSshSupervisor

MAX_MESSAGE_BYTES = 8 * 1024 * 1024
MAX_STDIN_BYTES = 4 * 1024 * 1024


def default_state_dir() -> Path:
    """Return a short private state directory suitable for Unix sockets."""

    if os.uname().sysname == "Darwin":
        return Path.home() / "Library" / "Caches" / "pazuzu"
    return Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "pazuzu"


def default_socket_path() -> Path:
    return default_state_dir() / "gateway.sock"


def default_control_path() -> Path:
    return default_state_dir() / "ssh.ctl"


def _encode(message: Mapping[str, Any]) -> bytes:
    try:
        encoded = json.dumps(message, separators=(",", ":"), allow_nan=False).encode() + b"\n"
    except (TypeError, ValueError) as exc:
        raise ProtocolError(f"message is not JSON serializable: {exc}") from exc
    if len(encoded) > MAX_MESSAGE_BYTES:
        raise ProtocolError("gateway message exceeds the 8 MiB limit")
    return encoded


def _decode(content: bytes) -> dict[str, Any]:
    try:
        message = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError("gateway message is not valid JSON") from exc
    if not isinstance(message, dict):
        raise ProtocolError("gateway message must be a JSON object")
    return message


async def call_gateway(
    socket_path: Path,
    operation: str,
    params: Mapping[str, Any] | None = None,
    *,
    timeout: float = 250.0,
) -> dict[str, Any]:
    """Make one cancellable request to the local gateway."""

    request_id = uuid.uuid4().hex[:16]
    request = {"v": 1, "id": request_id, "op": operation, "params": dict(params or {})}
    try:
        connection = asyncio.open_unix_connection(
            socket_path,
            limit=MAX_MESSAGE_BYTES + 1,
        )
        reader, writer = await asyncio.wait_for(connection, 5.0)
    except (OSError, TimeoutError) as exc:
        raise ProtocolError(f"Pazuzu gateway is unavailable at {socket_path}: {exc}") from exc
    try:
        writer.write(_encode(request))
        await writer.drain()
        async with asyncio.timeout(timeout):
            response = _decode(await reader.readline())
    except TimeoutError as exc:
        raise ProtocolError(f"Pazuzu gateway did not respond within {timeout:g}s") from exc
    finally:
        writer.close()
        with contextlib.suppress(OSError):
            await writer.wait_closed()
    if response.get("id") != request_id:
        raise ProtocolError("gateway response does not match the request")
    if response.get("ok") is True and isinstance(response.get("result"), dict):
        return response["result"]
    error = response.get("error")
    if response.get("ok") is False and isinstance(error, dict):
        raise PazuzuError(str(error.get("message") or "gateway operation failed"))
    raise ProtocolError("gateway returned an invalid response envelope")


async def execute_via_gateway(
    socket_path: Path,
    command: str,
    *,
    stdin: bytes = b"",
    timeout: float = 120.0,
    retry_safe: bool = False,
) -> dict[str, Any]:
    """Execute a bounded remote command through the local gateway."""

    if len(stdin) > MAX_STDIN_BYTES:
        raise ValueError("standard input exceeds the 4 MiB gateway limit")
    params = {
        "command": command,
        "stdin_base64": base64.b64encode(stdin).decode(),
        "timeout_seconds": timeout,
        "retry_safe": retry_safe,
    }
    return await call_gateway(socket_path, "execute", params, timeout=timeout + 140.0)


class GatewayServer:
    """Expose one supervisor without sharing its state with clients."""

    def __init__(self, supervisor: OpenSshSupervisor, socket_path: Path) -> None:
        self.supervisor = supervisor
        self.socket_path = socket_path
        self.stop_event = asyncio.Event()
        self.preserve_master = False

    def request_stop(self, preserve_master: bool = False) -> None:
        """Stop locally, optionally leaving SSH for a replacement gateway."""

        self.preserve_master = preserve_master
        self.stop_event.set()

    async def serve(self) -> None:
        """Serve until cancelled or a local shutdown request arrives."""

        self.socket_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        await self._prepare_socket()
        server = await asyncio.start_unix_server(
            self._handle_client,
            path=self.socket_path,
            limit=MAX_MESSAGE_BYTES + 1,
        )
        self.socket_path.chmod(0o600)
        await self.supervisor.start()
        try:
            async with server:
                await self.stop_event.wait()
        finally:
            server.close()
            await server.wait_closed()
            if self.preserve_master:
                await self.supervisor.detach()
            else:
                await self.supervisor.close()
            self.socket_path.unlink(missing_ok=True)

    async def _prepare_socket(self) -> None:
        if not self.socket_path.exists():
            return
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_unix_connection(self.socket_path), 1.0
            )
        except (OSError, TimeoutError):
            self.socket_path.unlink(missing_ok=True)
            return
        writer.close()
        with contextlib.suppress(OSError):
            await writer.wait_closed()
        del reader
        raise RuntimeError(f"another Pazuzu gateway is already using {self.socket_path}")

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        request_id: Any = None
        try:
            request = _decode(await reader.readline())
            request_id = request.get("id")
            task = asyncio.create_task(self._dispatch(request))
            disconnected = asyncio.create_task(reader.read(1))
            done, _ = await asyncio.wait(
                (task, disconnected), return_when=asyncio.FIRST_COMPLETED
            )
            if disconnected in done:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
                response = None
            else:
                disconnected.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await disconnected
                response = {"v": 1, "id": request_id, "ok": True, "result": task.result()}
        except asyncio.CancelledError:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()
            raise
        except (KeyError, OSError, PazuzuError, TypeError, ValueError) as exc:
            response = {
                "v": 1,
                "id": request_id,
                "ok": False,
                "error": {"type": type(exc).__name__, "message": str(exc)},
            }
        if response is not None:
            try:
                writer.write(_encode(response))
                await writer.drain()
            except (BrokenPipeError, ConnectionResetError):
                pass
        writer.close()
        with contextlib.suppress(OSError):
            await writer.wait_closed()

    async def _dispatch(self, request: dict[str, Any]) -> dict[str, Any]:
        if request.get("v") != 1 or not isinstance(request.get("id"), str):
            raise ProtocolError("invalid gateway request envelope")
        params = request.get("params")
        if not isinstance(params, dict):
            raise ProtocolError("request params must be an object")
        operation = request.get("op")
        if operation == "status":
            return await self.supervisor.health(probe=bool(params.get("probe", False)))
        if operation == "reconnect":
            return await self.supervisor.reconnect()
        if operation == "shutdown":
            asyncio.get_running_loop().call_soon(self.request_stop)
            return {"stopping": True}
        if operation in {"connection_attachment", "shell_attachment"}:
            if params:
                raise ProtocolError(f"{operation} params must be empty")
            return (await self.supervisor.connection_attachment()).as_dict()
        if operation == "execute":
            return await self._execute(params)
        raise ProtocolError(f"unknown gateway operation {operation!r}")

    async def _execute(self, params: dict[str, Any]) -> dict[str, Any]:
        encoded = params.get("stdin_base64", "")
        if not isinstance(encoded, str):
            raise TypeError("stdin_base64 must be a string")
        try:
            stdin = base64.b64decode(encoded, validate=True)
        except ValueError as exc:
            raise ValueError("stdin_base64 is invalid") from exc
        if len(stdin) > MAX_STDIN_BYTES:
            raise ValueError("standard input exceeds the 4 MiB gateway limit")
        command = params["command"]
        timeout = params.get("timeout_seconds", 120.0)
        retry_safe = params.get("retry_safe", False)
        if not isinstance(command, str) or isinstance(timeout, bool) or not isinstance(
            timeout, (int, float)
        ):
            raise TypeError("invalid execute parameters")
        if not isinstance(retry_safe, bool):
            raise TypeError("retry_safe must be a boolean")
        result = await self.supervisor.execute(
            command, stdin=stdin, timeout=float(timeout), retry_safe=retry_safe
        )
        return result.as_dict()


__all__ = [
    "GatewayServer",
    "call_gateway",
    "default_control_path",
    "default_socket_path",
    "execute_via_gateway",
]

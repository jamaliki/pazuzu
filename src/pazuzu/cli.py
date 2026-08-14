"""Command-line interface for the Pazuzu gateway."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
from pathlib import Path

from .errors import PazuzuError
from .gateway import (
    GatewayServer,
    call_gateway,
    default_control_path,
    default_socket_path,
    execute_via_gateway,
)
from .launchd import (
    install_bridge,
    install_services,
    remove_bridge,
    service_status,
    uninstall_services,
)
from .shell import require_interactive_terminal, run_shell
from .ssh import OpenSshSupervisor, SshSettings
from .transport import DEFAULT_MAX_SESSIONS, ShellAttachment, run_bridge


def _path(value: str) -> Path:
    return Path(value).expanduser().resolve()


def _socket_default() -> Path:
    return _path(os.environ.get("PAZUZU_SOCKET", str(default_socket_path())))


def _control_default() -> Path:
    return _path(os.environ.get("PAZUZU_CONTROL_PATH", str(default_control_path())))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pazuzu", description="supervise one resilient OpenSSH connection"
    )
    subparsers = parser.add_subparsers(dest="operation", required=True)

    serve = subparsers.add_parser("serve", help="run the local connection gateway")
    serve.add_argument("--host", default=os.environ.get("PAZUZU_HOST"), required=False)
    serve.add_argument("--socket", type=_path, default=_socket_default())
    serve.add_argument("--control-path", type=_path, default=_control_default())
    serve.add_argument("--ssh", default=os.environ.get("PAZUZU_SSH", "/usr/bin/ssh"))
    serve.add_argument("--max-sessions", type=int, default=DEFAULT_MAX_SESSIONS)
    serve.add_argument("--connect-timeout", type=float, default=60.0)
    serve.add_argument("--probe-interval", type=float, default=60.0)

    for name in ("status", "reconnect", "stop"):
        command = subparsers.add_parser(name)
        command.add_argument("--socket", type=_path, default=_socket_default())
        if name == "status":
            command.add_argument("--probe", action="store_true")

    execute = subparsers.add_parser("exec", help="run one command through the gateway")
    execute.add_argument("--socket", type=_path, default=_socket_default())
    execute.add_argument("--timeout", type=float, default=120.0)
    execute.add_argument(
        "--retry-safe",
        action="store_true",
        help="allow one replay after repair; only for idempotent operations",
    )
    execute.add_argument("command", nargs=argparse.REMAINDER)

    bridge = subparsers.add_parser(
        "bridge", help="forward one loopback port and run its remote service"
    )
    bridge.add_argument("--host", default=os.environ.get("PAZUZU_HOST"), required=False)
    bridge.add_argument("--control-path", type=_path, default=_control_default())
    bridge.add_argument("--ssh", default=os.environ.get("PAZUZU_SSH", "/usr/bin/ssh"))
    bridge.add_argument("--listen-host", default="127.0.0.1")
    bridge.add_argument("--listen-port", type=int, required=True)
    bridge.add_argument("--remote-host", default="127.0.0.1")
    bridge.add_argument("--remote-port", type=int, required=True)
    bridge.add_argument("remote_command", nargs=argparse.REMAINDER)

    shell = subparsers.add_parser(
        "shell", help="attach an interactive terminal to a persistent remote tmux session"
    )
    shell.add_argument("--socket", type=_path, default=_socket_default())
    shell.add_argument("--session", default="pazuzu")

    service = subparsers.add_parser("service", help="manage auto-restarting macOS services")
    service_commands = service.add_subparsers(dest="service_operation", required=True)
    install = service_commands.add_parser("install")
    install.add_argument("--host", default=os.environ.get("PAZUZU_HOST"), required=False)
    install.add_argument("--with-mcp", action="store_true")
    install.add_argument("--mcp-port", type=int, default=8767)
    install_bridge_command = service_commands.add_parser(
        "install-bridge", help="install an auto-restarting generic service bridge"
    )
    install_bridge_command.add_argument("name")
    install_bridge_command.add_argument("--listen-host", default="127.0.0.1")
    install_bridge_command.add_argument("--listen-port", type=int, required=True)
    install_bridge_command.add_argument("--remote-host", default="127.0.0.1")
    install_bridge_command.add_argument("--remote-port", type=int, required=True)
    install_bridge_command.add_argument("remote_command", nargs="+")
    remove_bridge_command = service_commands.add_parser("remove-bridge")
    remove_bridge_command.add_argument("name")
    service_commands.add_parser("status")
    service_commands.add_parser("uninstall")
    return parser


async def _serve(arguments: argparse.Namespace) -> None:
    if not arguments.host:
        raise ValueError("set --host or PAZUZU_HOST")
    settings = SshSettings(
        host=arguments.host,
        control_path=arguments.control_path,
        ssh_binary=arguments.ssh,
        connect_timeout=arguments.connect_timeout,
        probe_interval=arguments.probe_interval,
        max_sessions=arguments.max_sessions,
    )
    gateway = GatewayServer(OpenSshSupervisor(settings), arguments.socket)
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, gateway.request_stop, False)
    loop.add_signal_handler(signal.SIGTERM, gateway.request_stop, True)
    await gateway.serve()


def _remote_command(arguments: list[str]) -> str:
    remaining = arguments[1:] if arguments[:1] == ["--"] else arguments
    if not remaining:
        raise ValueError("provide a remote command after 'pazuzu exec --'")
    return " ".join(remaining)


def _read_stdin() -> bytes:
    if sys.stdin.isatty():
        return b""
    content = sys.stdin.buffer.read(4 * 1024 * 1024 + 1)
    if len(content) > 4 * 1024 * 1024:
        raise ValueError("standard input exceeds the 4 MiB gateway limit")
    return content


async def _run(arguments: argparse.Namespace) -> int:
    if arguments.operation == "service":
        if arguments.service_operation == "install":
            if not arguments.host:
                raise ValueError("set --host or PAZUZU_HOST")
            if not 1 <= arguments.mcp_port <= 65535:
                raise ValueError("mcp-port must be between 1 and 65535")
            installed = install_services(
                arguments.host,
                mcp_port=arguments.mcp_port if arguments.with_mcp else None,
            )
            print(json.dumps({"installed": [str(item) for item in installed]}, indent=2))
            return 0
        if arguments.service_operation == "install-bridge":
            target = install_bridge(
                arguments.name,
                _bridge_command(arguments.remote_command),
                listen_host=arguments.listen_host,
                listen_port=arguments.listen_port,
                remote_host=arguments.remote_host,
                remote_port=arguments.remote_port,
            )
            print(json.dumps({"installed": str(target)}, indent=2))
            return 0
        if arguments.service_operation == "remove-bridge":
            target = remove_bridge(arguments.name)
            print(json.dumps({"removed": None if target is None else str(target)}, indent=2))
            return 0
        if arguments.service_operation == "uninstall":
            removed = uninstall_services()
            print(json.dumps({"removed": [str(item) for item in removed]}, indent=2))
            return 0
        print(json.dumps(service_status(), indent=2, sort_keys=True))
        return 0
    if arguments.operation == "serve":
        await _serve(arguments)
        return 0
    if arguments.operation == "status":
        result = await call_gateway(
            arguments.socket, "status", {"probe": arguments.probe}, timeout=90.0
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result.get("connection") == "connected" else 1
    if arguments.operation == "reconnect":
        result = await call_gateway(arguments.socket, "reconnect", timeout=140.0)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    if arguments.operation == "stop":
        result = await call_gateway(arguments.socket, "shutdown", timeout=10.0)
        print(json.dumps(result, separators=(",", ":")))
        return 0
    if arguments.operation == "exec":
        result = await execute_via_gateway(
            arguments.socket,
            _remote_command(arguments.command),
            stdin=_read_stdin(),
            timeout=arguments.timeout,
            retry_safe=arguments.retry_safe,
        )
        sys.stdout.write(str(result["stdout"]))
        sys.stderr.write(str(result["stderr"]))
        if result.get("stdout_truncated") or result.get("stderr_truncated"):
            print("pazuzu: remote output was truncated", file=sys.stderr)
        return int(result["exit_code"])
    if arguments.operation == "shell":
        require_interactive_terminal()

        async def get_attachment() -> ShellAttachment:
            result = await call_gateway(arguments.socket, "shell_attachment", timeout=140.0)
            return ShellAttachment.from_dict(result)

        async def reconnect() -> dict[str, object]:
            return await call_gateway(arguments.socket, "reconnect", timeout=140.0)

        async def status() -> dict[str, object]:
            return await call_gateway(arguments.socket, "status")

        return await run_shell(
            arguments.session,
            get_attachment=get_attachment,
            reconnect=reconnect,
            status=status,
            stderr=sys.stderr,
        )
    raise AssertionError("unreachable operation")


def _bridge_command(arguments: list[str]) -> list[str]:
    command = arguments[1:] if arguments[:1] == ["--"] else arguments
    if not command:
        raise ValueError("provide a remote command after '--'")
    return command


def _exec_bridge(arguments: argparse.Namespace) -> int:
    if not arguments.host:
        raise ValueError("set --host or PAZUZU_HOST")
    settings = SshSettings(
        host=arguments.host,
        control_path=arguments.control_path,
        ssh_binary=arguments.ssh,
    )
    return run_bridge(
        settings,
        listen_host=arguments.listen_host,
        listen_port=arguments.listen_port,
        remote_host=arguments.remote_host,
        remote_port=arguments.remote_port,
        remote_command=_bridge_command(arguments.remote_command),
    )


def main(argv: list[str] | None = None) -> int:
    """Run the Pazuzu CLI."""

    parser = _parser()
    arguments = parser.parse_args(argv)
    try:
        if arguments.operation == "bridge":
            return _exec_bridge(arguments)
        return asyncio.run(_run(arguments))
    except KeyboardInterrupt:
        return 130
    except (OSError, PazuzuError, RuntimeError, TypeError, ValueError) as exc:
        parser.exit(2, f"pazuzu: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())

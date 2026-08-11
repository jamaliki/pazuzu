"""Install the local gateway and MCP adapter as macOS LaunchAgents."""

from __future__ import annotations

import contextlib
import os
import plistlib
import re
import subprocess
import sys
import time
from pathlib import Path

from .gateway import default_control_path, default_socket_path, default_state_dir

GATEWAY_LABEL = "science.jamali.pazuzu.gateway"
MCP_LABEL = "science.jamali.pazuzu.mcp"
BRIDGE_LABEL_PREFIX = "science.jamali.pazuzu.bridge."
BRIDGE_NAME = re.compile(r"[a-z0-9][a-z0-9-]{0,31}\Z")


def _domain() -> str:
    return f"gui/{os.getuid()}"


def _agent_path(label: str) -> Path:
    return Path.home() / "Library" / "LaunchAgents" / f"{label}.plist"


def _bridge_label(name: str) -> str:
    if not BRIDGE_NAME.fullmatch(name):
        raise ValueError("bridge name must be 1-32 lowercase letters, digits, or hyphens")
    return f"{BRIDGE_LABEL_PREFIX}{name}"


def _executable(name: str) -> Path:
    candidate = Path(sys.argv[0]).resolve().with_name(name)
    if not candidate.exists():
        raise RuntimeError(
            f"could not find {name!r} beside {Path(sys.argv[0]).resolve()}; "
            "install Pazuzu with `uv tool install '.[mcp]'` first"
        )
    return candidate


def _plist(label: str, arguments: list[str], log_name: str) -> dict[str, object]:
    state_dir = default_state_dir()
    state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    state_dir.chmod(0o700)
    return {
        "Label": label,
        "ProgramArguments": arguments,
        "RunAtLoad": True,
        "KeepAlive": True,
        "ProcessType": "Background",
        "ThrottleInterval": 5,
        "Umask": 0o077,
        "StandardOutPath": str(state_dir / f"{log_name}.stdout.log"),
        "StandardErrorPath": str(state_dir / f"{log_name}.stderr.log"),
    }


def _write_plist(label: str, content: dict[str, object]) -> Path:
    target = _agent_path(label)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".tmp")
    with temporary.open("wb") as handle:
        plistlib.dump(content, handle, sort_keys=True)
    temporary.chmod(0o600)
    temporary.replace(target)
    return target


def _launchctl(*arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ("/bin/launchctl", *arguments),
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def _loaded(label: str) -> bool:
    return _launchctl("print", f"{_domain()}/{label}", check=False).returncode == 0


def _wait_until_unloaded(label: str, timeout: float = 5.0) -> None:
    """Wait for asynchronous launchd teardown before reusing a label."""

    deadline = time.monotonic() + timeout
    while _loaded(label):
        if time.monotonic() >= deadline:
            raise RuntimeError(f"launchd did not unload {label} within {timeout:g}s")
        time.sleep(0.05)


def _bootstrap(label: str, target: Path) -> None:
    """Tolerate launchd's brief EIO window after bootout."""

    failures: list[str] = []
    for attempt in range(5):
        result = _launchctl("bootstrap", _domain(), str(target), check=False)
        if result.returncode == 0 or _loaded(label):
            return
        failures.append(result.stdout.strip() or f"exit code {result.returncode}")
        time.sleep(0.2 * (attempt + 1))
    raise RuntimeError(f"could not load {label}: {failures[-1]}")


def _install(label: str, content: dict[str, object]) -> Path:
    target = _write_plist(label, content)
    _launchctl("bootout", f"{_domain()}/{label}", check=False)
    _wait_until_unloaded(label)
    _bootstrap(label, target)
    return target


def _configured_host() -> str | None:
    target = _agent_path(GATEWAY_LABEL)
    if not target.exists():
        return None
    try:
        with target.open("rb") as handle:
            content = plistlib.load(handle)
    except (OSError, plistlib.InvalidFileException):
        return None
    arguments = content.get("ProgramArguments", [])
    if not isinstance(arguments, list) or "--host" not in arguments:
        return None
    index = arguments.index("--host") + 1
    return str(arguments[index]) if index < len(arguments) else None


def _stop_detached_master(host: str | None) -> None:
    control_path = default_control_path()
    if host is None or not control_path.exists():
        return
    with contextlib.suppress(OSError, subprocess.TimeoutExpired):
        subprocess.run(
            (
                "/usr/bin/ssh",
                "-S",
                str(control_path),
                "-o",
                "ProxyCommand=/usr/bin/false",
                "-O",
                "exit",
                host,
            ),
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5.0,
        )


def install_services(host: str, *, mcp_port: int | None = None) -> list[Path]:
    """Install an auto-restarting gateway and optional MCP adapter."""

    if sys.platform != "darwin":
        raise RuntimeError("LaunchAgent installation is available only on macOS")
    gateway_arguments = [
        str(_executable("pazuzu")),
        "serve",
        "--host",
        host,
        "--socket",
        str(default_socket_path()),
        "--control-path",
        str(default_control_path()),
    ]
    installed = [_install(GATEWAY_LABEL, _plist(GATEWAY_LABEL, gateway_arguments, "gateway"))]
    if mcp_port is not None:
        mcp_arguments = [
            str(_executable("pazuzu-mcp")),
            "--socket",
            str(default_socket_path()),
            "--transport",
            "streamable-http",
            "--port",
            str(mcp_port),
        ]
        installed.append(_install(MCP_LABEL, _plist(MCP_LABEL, mcp_arguments, "mcp")))
    return installed


def install_bridge(
    name: str,
    remote_command: list[str],
    *,
    listen_host: str,
    listen_port: int,
    remote_host: str,
    remote_port: int,
) -> Path:
    """Install a generic service and port-forward channel over Pazuzu's master."""

    host = _configured_host()
    if not host:
        raise RuntimeError("install the Pazuzu gateway before installing a bridge")
    if not remote_command:
        raise ValueError("remote command must not be empty")
    label = _bridge_label(name)
    arguments = [
        str(_executable("pazuzu")),
        "bridge",
        "--host",
        host,
        "--control-path",
        str(default_control_path()),
        "--listen-host",
        listen_host,
        "--listen-port",
        str(listen_port),
        "--remote-host",
        remote_host,
        "--remote-port",
        str(remote_port),
        "--",
        *remote_command,
    ]
    return _install(label, _plist(label, arguments, f"bridge-{name}"))


def remove_bridge(name: str) -> Path | None:
    """Unload and remove one named bridge."""

    label = _bridge_label(name)
    target = _agent_path(label)
    _launchctl("bootout", f"{_domain()}/{label}", check=False)
    _wait_until_unloaded(label)
    if not target.exists():
        return None
    target.unlink()
    return target


def _bridge_labels() -> list[str]:
    directory = Path.home() / "Library" / "LaunchAgents"
    return [target.stem for target in sorted(directory.glob(f"{BRIDGE_LABEL_PREFIX}*.plist"))]


def uninstall_services() -> list[Path]:
    """Unload and remove Pazuzu's LaunchAgents, leaving no daemon running."""

    host = _configured_host()
    removed: list[Path] = []
    for label in (*_bridge_labels(), MCP_LABEL, GATEWAY_LABEL):
        target = _agent_path(label)
        _launchctl("bootout", f"{_domain()}/{label}", check=False)
        _wait_until_unloaded(label)
        if target.exists():
            target.unlink()
            removed.append(target)
    _stop_detached_master(host)
    return removed


def service_status() -> dict[str, str]:
    """Return launchd's compact state for both optional services."""

    statuses: dict[str, str] = {}
    for label in (GATEWAY_LABEL, MCP_LABEL, *_bridge_labels()):
        statuses[label] = "loaded" if _loaded(label) else "not_loaded"
    return statuses


__all__ = [
    "install_bridge",
    "install_services",
    "remove_bridge",
    "service_status",
    "uninstall_services",
]

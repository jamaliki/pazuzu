from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pazuzu import launchd


def result(exit_code: int, output: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess([], exit_code, output)


class LaunchdTests(unittest.TestCase):
    @mock.patch("pazuzu.launchd.time.sleep")
    @mock.patch("pazuzu.launchd._launchctl")
    def test_install_waits_for_unload_and_retries_bootstrap(
        self, launchctl: mock.Mock, _sleep: mock.Mock
    ) -> None:
        launchctl.side_effect = [
            result(0),  # bootout
            result(0),  # still loaded
            result(1),  # unloaded
            result(5, "Input/output error"),
            result(1),  # not loaded after failed bootstrap
            result(0),
        ]

        with mock.patch("pazuzu.launchd._write_plist", return_value=Path("agent.plist")):
            installed = launchd._install("test.service", {})

        self.assertEqual(Path("agent.plist"), installed)
        self.assertEqual(6, launchctl.call_count)

    @mock.patch("pazuzu.launchd._bridge_labels", return_value=["bridge"])
    @mock.patch("pazuzu.launchd._ready", side_effect=[True, False, False])
    @mock.patch("pazuzu.launchd._loaded", side_effect=[True, True, False])
    def test_service_status_distinguishes_ready_waiting_and_unloaded(
        self, _loaded: mock.Mock, _ready: mock.Mock, _bridges: mock.Mock
    ) -> None:
        self.assertEqual(
            {
                launchd.GATEWAY_LABEL: "ready",
                launchd.MCP_LABEL: "waiting",
                "bridge": "not_loaded",
            },
            launchd.service_status(),
        )

    @mock.patch("pazuzu.launchd._install", side_effect=lambda label, content: content)
    @mock.patch("pazuzu.launchd._executable", side_effect=lambda name: Path("/bin") / name)
    def test_gateway_and_mcp_are_not_background_jobs(
        self, _executable: mock.Mock, _install: mock.Mock
    ) -> None:
        with (
            tempfile.TemporaryDirectory() as state,
            mock.patch("pazuzu.launchd.default_state_dir", return_value=Path(state)),
            mock.patch("pazuzu.launchd.sys.platform", "darwin"),
        ):
            gateway, mcp = launchd.install_services("example-host", mcp_port=8767, max_sessions=10)

        # Background jobs get LEDBAT congestion control on every socket.
        self.assertEqual("Standard", gateway["ProcessType"])
        self.assertEqual("Standard", mcp["ProcessType"])
        arguments = gateway["ProgramArguments"]
        self.assertEqual("10", arguments[arguments.index("--max-sessions") + 1])

    @mock.patch("pazuzu.launchd._install", side_effect=lambda label, content: content)
    @mock.patch("pazuzu.launchd._executable", side_effect=lambda name: Path("/bin") / name)
    def test_bridge_uses_the_installed_gateway_host_in_the_background(
        self, _executable: mock.Mock, _install: mock.Mock
    ) -> None:
        gateway = ["/bin/pazuzu", "serve", "--host", "example-host"]
        with (
            tempfile.TemporaryDirectory() as state,
            mock.patch("pazuzu.launchd.default_state_dir", return_value=Path(state)),
            mock.patch("pazuzu.launchd._program_arguments", return_value=gateway),
        ):
            bridge = launchd.install_bridge(
                "queue",
                ["/remote/bin/server"],
                listen_host="127.0.0.1",
                listen_port=8766,
                remote_host="127.0.0.1",
                remote_port=18766,
            )

        arguments = bridge["ProgramArguments"]
        self.assertEqual("example-host", arguments[arguments.index("--host") + 1])
        self.assertEqual("Background", bridge["ProcessType"])


if __name__ == "__main__":
    unittest.main()

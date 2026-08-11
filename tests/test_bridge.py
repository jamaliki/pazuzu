from __future__ import annotations

import unittest
from pathlib import Path
from unittest import mock

from pazuzu import launchd
from pazuzu.cli import _parser
from pazuzu.transport import SshSettings, bridge_argv


class BridgeTests(unittest.TestCase):
    def test_install_parser_accepts_options_after_the_bridge_name(self) -> None:
        arguments = _parser().parse_args(
            [
                "service",
                "install-bridge",
                "queue",
                "--listen-port",
                "8766",
                "--remote-port",
                "18766",
                "--",
                "/remote/bin/server",
                "--flag",
            ]
        )

        self.assertEqual("queue", arguments.name)
        self.assertEqual(8766, arguments.listen_port)
        self.assertEqual(["/remote/bin/server", "--flag"], arguments.remote_command)

    def test_bridge_reuses_the_master_without_direct_fallback(self) -> None:
        arguments = bridge_argv(
            SshSettings(host="example-host", control_path=Path("/tmp/pazuzu.ctl")),
            listen_host="127.0.0.1",
            listen_port=8766,
            remote_host="127.0.0.1",
            remote_port=18766,
            remote_command=["/remote/bin/server", "--name", "value with spaces"],
        )

        self.assertEqual("/usr/bin/ssh", arguments[0])
        self.assertIn("ProxyCommand=/usr/bin/false", arguments)
        self.assertIn("ExitOnForwardFailure=yes", arguments)
        self.assertIn("127.0.0.1:8766:127.0.0.1:18766", arguments)
        self.assertEqual("example-host", arguments[-2])
        self.assertEqual(
            "exec /remote/bin/server --name 'value with spaces'", arguments[-1]
        )

    @mock.patch("pazuzu.launchd._install")
    @mock.patch("pazuzu.launchd._executable", return_value=Path("/local/bin/pazuzu"))
    @mock.patch("pazuzu.launchd._configured_host", return_value="example-host")
    def test_launch_agent_uses_the_configured_gateway(
        self, _host: mock.Mock, _executable: mock.Mock, install: mock.Mock
    ) -> None:
        install.return_value = Path("bridge.plist")

        result = launchd.install_bridge(
            "queue",
            ["/remote/bin/server", "--port", "18766"],
            listen_host="127.0.0.1",
            listen_port=8766,
            remote_host="127.0.0.1",
            remote_port=18766,
        )

        self.assertEqual(Path("bridge.plist"), result)
        label, content = install.call_args.args
        self.assertEqual("science.jamali.pazuzu.bridge.queue", label)
        arguments = content["ProgramArguments"]
        self.assertEqual("example-host", arguments[arguments.index("--host") + 1])
        self.assertEqual(
            ["--", "/remote/bin/server", "--port", "18766"], arguments[-4:]
        )

    def test_bridge_names_are_safe_launchd_suffixes(self) -> None:
        with self.assertRaisesRegex(ValueError, "lowercase"):
            launchd._bridge_label("Queue/One")


if __name__ == "__main__":
    unittest.main()

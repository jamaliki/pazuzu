from __future__ import annotations

import unittest
from pathlib import Path
from unittest import mock

from pazuzu import launchd
from pazuzu.cli import _parser
from pazuzu.errors import ConnectionUnavailable
from pazuzu.transport import (
    SshSettings,
    bridge_control_argv,
    bridge_session_argv,
    run_bridge,
)


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
        settings = SshSettings(host="example-host", control_path=Path("/tmp/pazuzu.ctl"))
        control = bridge_control_argv(
            settings,
            operation="forward",
            listen_host="127.0.0.1",
            listen_port=8766,
            remote_host="127.0.0.1",
            remote_port=18766,
        )
        session = bridge_session_argv(
            settings,
            remote_command=["/remote/bin/server", "--name", "value with spaces"],
        )

        self.assertEqual("/usr/bin/ssh", control[0])
        self.assertIn("ProxyCommand=/usr/bin/false", control)
        self.assertIn("ExitOnForwardFailure=yes", control)
        self.assertIn("127.0.0.1:8766:127.0.0.1:18766", control)
        self.assertEqual("forward", control[control.index("-O") + 1])
        self.assertEqual("example-host", session[-2])
        self.assertIn("exec sh -c", session[-1])
        self.assertIn("exec 3<&0", session[-1])
        self.assertIn("read -r _ <&3", session[-1])
        self.assertIn("pazuzu-bridge /remote/bin/server --name 'value with spaces'", session[-1])

    @mock.patch("pazuzu.transport.signal.signal")
    @mock.patch("pazuzu.transport.subprocess.Popen")
    @mock.patch("pazuzu.transport.subprocess.run")
    def test_bridge_cleans_stale_and_final_forwards(
        self, run: mock.Mock, popen: mock.Mock, _signal: mock.Mock
    ) -> None:
        run.return_value = mock.Mock(returncode=0, stdout=b"")
        popen.return_value.wait.return_value = 17

        result = run_bridge(
            SshSettings(host="example-host", control_path=Path("/tmp/pazuzu.ctl")),
            listen_host="127.0.0.1",
            listen_port=8766,
            remote_host="127.0.0.1",
            remote_port=18766,
            remote_command=["/remote/bin/server"],
        )

        self.assertEqual(17, result)
        self.assertEqual(3, run.call_count)
        self.assertEqual(mock.call(mock.ANY, stdin=-1), popen.call_args)
        operations = [call.args[0][call.args[0].index("-O") + 1] for call in run.call_args_list]
        self.assertEqual(["cancel", "forward", "cancel"], operations)

    @mock.patch("pazuzu.transport.subprocess.Popen")
    @mock.patch("pazuzu.transport.subprocess.run")
    def test_bridge_does_not_start_service_when_forward_fails(
        self, run: mock.Mock, popen: mock.Mock
    ) -> None:
        run.side_effect = [
            mock.Mock(returncode=255, stdout=b"nothing to cancel"),
            mock.Mock(returncode=255, stdout=b"forward refused"),
        ]

        with self.assertRaisesRegex(ConnectionUnavailable, "forward refused"):
            run_bridge(
                SshSettings(host="example-host", control_path=Path("/tmp/pazuzu.ctl")),
                listen_host="127.0.0.1",
                listen_port=8766,
                remote_host="127.0.0.1",
                remote_port=18766,
                remote_command=["/remote/bin/server"],
            )

        popen.assert_not_called()

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
